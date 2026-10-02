pub type Result<T> = std::result::Result<T, ScriptErrorKind>;

#[derive(Debug, Clone)]
pub struct ScriptError {
    kind: Box<ScriptErrorKind>,
}

#[derive(Debug, Clone)]
pub enum ScriptErrorKind {
    CustomError { message: String },
    UnknownError,
}
