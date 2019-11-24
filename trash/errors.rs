use std::fmt;

pub type TerastashResult<T> = Result<T, Box<TerastashError>>;

// =============================================================================
// TerastashError trait

pub trait TerastashError: Error + Send + 'static {
    fn is_human(&self) -> bool { false }
    fn terastash_cause(&self) -> Option<&TerastashError>{ None }
}

impl Error for Box<TerastashError> {
    fn description(&self) -> &str { (**self).description() }
    fn cause(&self) -> Option<&Error> { (**self).cause() }
}

impl TerastashError for Box<TerastashError> {
    fn is_human(&self) -> bool { (**self).is_human() }
    fn terastash_cause(&self) -> Option<&TerastashError> { (**self).terastash_cause() }
}

// =============================================================================
// Concrete errors

struct ConcreteTerastashError {
    description: String,
    detail: Option<String>,
    cause: Option<Box<Error+Send>>,
    is_human: bool,
}

impl fmt::Display for ConcreteTerastashError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "{}", self.description));
        if let Some(ref s) = self.detail {
            try!(write!(f, " ({})", s));
        }
        Ok(())
    }
}
impl fmt::Debug for ConcreteTerastashError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl Error for ConcreteTerastashError {
    fn description(&self) -> &str { &self.description }
    fn cause(&self) -> Option<&Error> {
        self.cause.as_ref().map(|c| {
            let e: &Error = &**c; e
        })
    }
}

impl TerastashError for ConcreteTerastashError {
    fn is_human(&self) -> bool {
        self.is_human
    }
}

pub fn human<S: fmt::Display>(error: S) -> Box<TerastashError> {
    Box::new(ConcreteTerastashError {
        description: error.to_string(),
        detail: None,
        cause: None,
        is_human: true
    })
}
