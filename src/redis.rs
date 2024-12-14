use failure::{format_err, Error};
use std::{
    borrow::BorrowMut,
    io::{self, Bytes},
    rc::Rc,
};

// TODO: I will probably have to end up giving lifetime params here
#[derive(Debug, PartialEq)]
pub enum DataTypes {
    // For now .. bulk string and string
    // have the same representation
    //
    // TODO: if the string doesn't require mutation .. use a different type instead..?
    String(Rc<str>),
    ErrorMessage(Rc<str>),
    Int(i64),
    Array(Vec<DataTypes>),
    NullArray,
    NullBulkString,
    Boolean(bool),
    Double(f64),
    // TODO: Support more complex data tyeps in Redis
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Ping,
    Echo(Rc<str>), // TODO: prob want to use a better type here..
}

fn get_line<T: io::Read>(r: &mut Bytes<T>) -> Result<Vec<u8>, Error> {
    let mut x: Vec<u8> = vec![];

    // borrow_mut is needed to create a fresh borrow
    for s in r.borrow_mut() {
        let c = s?;
        if let b'\r' = c {
            break;
        }
        x.push(c);
    }

    return Ok(x);
}

/// Handles a redis request by iterating over the bytes, consuming the iterator
pub fn parse_redis_request<T>(request: &mut Bytes<T>) -> Result<DataTypes, Error>
where
    T: io::Read,
{
    // read the first byte to see what type it is
    let s = request.next().ok_or(format_err!("Stream ended early"))??;
    println!("Processing: {}", s as char);

    let r = match s {
        b'+' => {
            let x = get_line(request)?;
            let utf8 = String::from_utf8(x)?;
            Ok(DataTypes::String(utf8.into()))
        }
        b':' => {
            let x = get_line(request)?;
            let utf8 = std::str::from_utf8(&x)?;
            let num = i64::from_str_radix(&utf8, 10)
                .map_err(|_e| format_err!("invalid digit: {}", utf8))?;
            Ok(DataTypes::Int(num))
        }
        b'$' => {
            let len_bytes = get_line(request)?;
            let len_str = std::str::from_utf8(&len_bytes)?;
            let len = u64::from_str_radix(len_str, 10)?;
            let mut str = Vec::with_capacity(len as usize);

            let n = request
                .next()
                .ok_or(format_err!("Stream ended early, missing LINEFEED."))??;
            if n != b'\n' {
                return Err(format_err!("Stream did not end with a LINEFEED"));
            }
            for cr in request.take(len as usize) {
                let c = cr?;
                str.push(c);
            }

            // parse the extra carraige return here
            let n = request
                .next()
                .ok_or(format_err!("Stream ended early, missing CARRAIGE RETURN."))??;
            if n != b'\r' {
                return Err(format_err!("Stream did not end with a CARRAIGE RETURN."));
            }

            let utf8 = String::from_utf8(str)?.into();
            Ok(DataTypes::String(utf8))
        }
        b'*' => {
            let len_bytes = get_line(request)?;
            let len_str = String::from_utf8(len_bytes)?;
            let len = u64::from_str_radix(&len_str, 10)
                .map_err(|_e| format_err!("Invalid digit: {len_str}"))?;
            let mut v = Vec::with_capacity(len as usize);

            let n = request
                .next()
                .ok_or(format_err!("Stream ended early, missing LINEFEED."))??;
            if n != b'\n' {
                return Err(format_err!("Stream did not end with a LINEFEED"));
            }
            for _ in 0..len {
                let d = parse_redis_request(request)?;
                v.push(d);
            }
            Ok(DataTypes::Array(v))
        }
        _ => Err(format_err!("Invalid datatype specifier")),
    };

    let n = request
        .next()
        .ok_or(format_err!("Stream ended early, missing LINEFEED."))??;
    if let b'\n' = n {
        r
    } else {
        Err(format_err!("Stream did not end with a LINEFEED"))
    }
}

pub fn into_command(d: DataTypes) -> Result<Command, Error> {
    match d {
        DataTypes::Array(v) => {
            let command = v.first().ok_or(format_err!("Command array is empty"))?;
            match command {
                DataTypes::String(s) => match s.to_uppercase().as_str() {
                    "ECHO" => {
                        let arg = v.get(1).ok_or(format_err!("Expected ECHO argument"))?;
                        if let DataTypes::String(s) = arg {
                            // TODO: this clones the string, which is not great
                            Ok(Command::Echo(s.clone()))
                        } else {
                            Err(format_err!("Echo argument not a string"))
                        }
                    }
                    "PING" => Ok(Command::Ping),
                    _ => Err(format_err!("Unknown command")),
                },
                _ => Err(format_err!("Command is not a string")),
            }
        }
        _ => Err(format_err!(
            "Could not convert into command because datatype is not an array"
        )),
    }
}

#[cfg(test)]
mod test {
    use io::Read;

    use super::*;

    // TODO: this was really easy to parse .. but probably want more thorough tests.
    #[test]
    fn test_parse_redis_request_string() {
        let bytes = "+hello world\r\n".as_bytes();
        let mut bytes_iter = bytes.bytes();
        let r = parse_redis_request(&mut bytes_iter);
        assert!(r.is_ok(), "r wasn't true, error is: {}", r.unwrap_err());
        assert_eq!(
            r.unwrap(),
            DataTypes::String(String::from("hello world").into())
        );
    }
    #[test]
    fn test_parse_redis_array() {
        let bytes = "*2\r\n:12345\r\n+hello world this is foo bar\r\n\n".as_bytes();
        let mut bytes_iter = bytes.bytes();
        let r = parse_redis_request(&mut bytes_iter);
        assert!(r.is_ok(), "r wasn't true, error is: {}", r.unwrap_err());
        if let Ok(DataTypes::Array(a)) = r {
            assert_eq!(a.len(), 2);
            assert_eq!(a[0], DataTypes::Int(12345));
            assert_eq!(
                a[1],
                DataTypes::String(String::from("hello world this is foo bar").into())
            );
        } else {
            assert!(false, "r wasn't an array");
        }
    }

    #[test]
    fn test_parse_redis_request_big_string() {
        let bytes = "$5\r\nhello\r\n\n".as_bytes();
        let mut bytes_iter = bytes.bytes();
        let r = parse_redis_request(&mut bytes_iter);
        assert!(r.is_ok(), "r wasn't true, error is: {}", r.unwrap_err());
        assert_eq!(r.unwrap(), DataTypes::String(String::from("hello").into()));
    }

    #[test]
    fn test_parse_request() {
        let bytes = "*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n\n".as_bytes();
        let mut bytes_iter = bytes.bytes();
        let r = parse_redis_request(&mut bytes_iter);
        if r.is_err() {
            let e = r.as_ref().unwrap_err();
            let bt = e.backtrace();
            println!("Backtrace: {bt:?}");
        }
        assert!(r.is_ok(), "r wasn't true, error is: {}", r.unwrap_err());
        if let Ok(DataTypes::Array(a)) = r {
            assert_eq!(a.len(), 2);
            assert_eq!(a[0], DataTypes::String(String::from("ECHO").into()));
            assert_eq!(a[1], DataTypes::String(String::from("hey").into()));
        } else {
            assert!(false, "r wasn't an array");
        }
    }
}
