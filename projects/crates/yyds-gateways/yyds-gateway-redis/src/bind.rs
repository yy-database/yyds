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
    else if command.eq_ignore_ascii_case(b"SET") {
        if arguments.len() < 3 {
            return Err(BindError::WrongArity);
        }
        if arguments.len() > 3 {
            return Err(BindError::UnsupportedOptions);
        }
        KeyValueAction::Put(arguments[2].to_vec())
    }
    else if command.eq_ignore_ascii_case(b"DEL") {
        if arguments.len() != 2 {
            return Err(BindError::WrongArity);
        }
        KeyValueAction::Delete
    }
    else {
        return Err(BindError::UnsupportedCommand);
    };
    Ok(KeyValueCommand { namespace: namespace.clone(), key: arguments[1].to_vec(), action })
}
