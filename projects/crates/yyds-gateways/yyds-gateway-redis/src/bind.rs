//! Redis storage command binding without direct shard access.

use yyds_types::{KeyValueAction, KeyValueCommand, Namespace};

/// A storage command rejected before execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindError {
    /// No command token was supplied.
    EmptyCommand,
    /// This command has no supported execution binding.
    UnsupportedCommand,
    /// Argument count differs from the supported command shape.
    WrongArity,
    /// SET options require semantics that are not implemented yet.
    UnsupportedOptions,
    /// Increment delta is not a canonical signed 64-bit integer.
    InvalidInteger,
}

/// Binds one binary Redis request into a protocol-neutral YYDS command.
pub fn bind(arguments: &[&[u8]], namespace: &Namespace) -> Result<KeyValueCommand, BindError> {
    let command = arguments.first().ok_or(BindError::EmptyCommand)?;
    let action = if command.eq_ignore_ascii_case(b"GET") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        KeyValueAction::Get
    }
    else if command.eq_ignore_ascii_case(b"EXISTS") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        KeyValueAction::Exists
    }
    else if command.eq_ignore_ascii_case(b"GETDEL") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        KeyValueAction::GetDelete
    }
    else if command.eq_ignore_ascii_case(b"SET") {
        if arguments.len() < 3 {
            return Err(BindError::WrongArity);
        }
        if arguments.len() == 4 && arguments[3].eq_ignore_ascii_case(b"NX") {
            KeyValueAction::PutIfAbsent(arguments[2].to_vec())
        }
        else if arguments.len() > 3 {
            return Err(BindError::UnsupportedOptions);
        }
        else {
            KeyValueAction::Put(arguments[2].to_vec())
        }
    }
    else if command.eq_ignore_ascii_case(b"DEL") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        KeyValueAction::Delete
    }
    else if command.eq_ignore_ascii_case(b"INCR") || command.eq_ignore_ascii_case(b"INCRBY") {
        let increment = command.eq_ignore_ascii_case(b"INCR");
        if arguments.len() != if increment { 2 } else { 3 } {
            return Err(BindError::WrongArity);
        }
        let delta = if increment {
            1
        }
        else {
            let text = std::str::from_utf8(arguments[2]).map_err(|_| BindError::InvalidInteger)?;
            let value = text.parse::<i64>().map_err(|_| BindError::InvalidInteger)?;
            if value.to_string() != text {
                return Err(BindError::InvalidInteger);
            }
            value
        };
        KeyValueAction::IncrementBy(delta)
    }
    else {
        return Err(BindError::UnsupportedCommand);
    };
    Ok(KeyValueCommand { namespace: namespace.clone(), key: arguments[1].to_vec(), action })
}
