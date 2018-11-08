use general_derive::{FromValue, ProcessValue, ToValue};

use super::*;
use crate::processor::FromValue;

/// POSIX signal with optional extended data.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct CError {
    /// The error code as specified by ISO C99, POSIX.1-2001 or POSIX.1-2008.
    #[metastructure(required = "true")]
    pub number: Annotated<i64>,

    /// Optional name of the errno constant.
    pub name: Annotated<String>,
}

/// Mach exception information.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct MachException {
    /// The mach exception type.
    #[metastructure(field = "exception", required = "true")]
    pub ty: Annotated<i64>,

    /// The mach exception code.
    #[metastructure(required = "true")]
    pub code: Annotated<u64>,

    /// The mach exception subcode.
    #[metastructure(required = "true")]
    pub subcode: Annotated<u64>,

    /// Optional name of the mach exception.
    pub name: Annotated<String>,
}

/// POSIX signal with optional extended data.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct PosixSignal {
    /// The POSIX signal number.
    #[metastructure(required = "true")]
    pub number: Annotated<i64>,

    /// An optional signal code present on Apple systems.
    pub code: Annotated<i64>,

    /// Optional name of the errno constant.
    pub name: Annotated<String>,

    /// Optional name of the errno constant.
    pub code_name: Annotated<String>,
}

/// Operating system or runtime meta information to an exception mechanism.
#[derive(Debug, Clone, PartialEq, Default, FromValue, ToValue, ProcessValue)]
pub struct MechanismMeta {
    /// Optional ISO C standard error code.
    pub errno: Annotated<CError>,

    /// Optional POSIX signal number.
    pub signal: Annotated<PosixSignal>,

    /// Optional mach exception information.
    pub mach_exception: Annotated<MachException>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

/// The mechanism by which an exception was generated and handled.
#[derive(Debug, Clone, PartialEq, Default, ToValue, ProcessValue)]
pub struct Mechanism {
    /// Mechanism type (required).
    #[metastructure(field = "type", required = "true", nonempty = "true", cap_size = "enumlike")]
    pub ty: Annotated<String>,

    /// Human readable detail description.
    #[metastructure(pii_kind = "freeform", cap_size = "message")]
    pub description: Annotated<String>,

    /// Link to online resources describing this error.
    #[metastructure(required = "false", nonempty = "true", cap_size = "path")]
    pub help_link: Annotated<String>,

    /// Flag indicating whether this exception was handled.
    pub handled: Annotated<bool>,

    /// Additional attributes depending on the mechanism type.
    #[metastructure(pii_kind = "databag")]
    // TODO: Cap?
    pub data: Annotated<Object<Value>>,

    /// Operating system or runtime meta information.
    pub meta: Annotated<MechanismMeta>,

    /// Additional arbitrary fields for forwards compatibility.
    #[metastructure(additional_properties)]
    pub other: Object<Value>,
}

impl FromValue for Mechanism {
    fn from_value(annotated: Annotated<Value>) -> Annotated<Self> {
        #[derive(FromValue)]
        struct NewMechanism {
            #[metastructure(field = "type", required = "true")]
            pub ty: Annotated<String>,
            pub description: Annotated<String>,
            pub help_link: Annotated<String>,
            pub handled: Annotated<bool>,
            pub data: Annotated<Object<Value>>,
            pub meta: Annotated<MechanismMeta>,
            #[metastructure(additional_properties)]
            pub other: Object<Value>,
        }

        #[derive(FromValue)]
        struct LegacyPosixSignal {
            #[metastructure(required = "true")]
            pub signal: Annotated<i64>,
            pub code: Annotated<i64>,
            pub name: Annotated<String>,
            pub code_name: Annotated<String>,
        }

        #[derive(FromValue)]
        struct LegacyMachException {
            #[metastructure(required = "true")]
            pub exception: Annotated<i64>,
            #[metastructure(required = "true")]
            pub code: Annotated<u64>,
            #[metastructure(required = "true")]
            pub subcode: Annotated<u64>,
            pub exception_name: Annotated<String>,
        }

        #[derive(FromValue)]
        struct LegacyMechanism {
            posix_signal: Annotated<LegacyPosixSignal>,
            mach_exception: Annotated<LegacyMachException>,
            #[metastructure(additional_properties)]
            pub other: Object<Value>,
        }

        match annotated {
            Annotated(Some(Value::Object(object)), meta) => {
                if object.is_empty() {
                    Annotated(None, meta)
                } else if object.contains_key("type") {
                    let annotated = Annotated(Some(Value::Object(object)), meta);
                    NewMechanism::from_value(annotated).map_value(|mechanism| Mechanism {
                        ty: mechanism.ty,
                        description: mechanism.description,
                        help_link: mechanism.help_link,
                        handled: mechanism.handled,
                        data: mechanism.data,
                        meta: mechanism.meta,
                        other: mechanism.other,
                    })
                } else {
                    let annotated = Annotated(Some(Value::Object(object)), meta);
                    LegacyMechanism::from_value(annotated).map_value(|legacy| Mechanism {
                        ty: Annotated::new("generic".to_string()),
                        description: Annotated::empty(),
                        help_link: Annotated::empty(),
                        handled: Annotated::empty(),
                        data: Annotated::new(legacy.other),
                        meta: Annotated::new(MechanismMeta {
                            errno: Annotated::empty(),
                            signal: legacy.posix_signal.map_value(|legacy| PosixSignal {
                                number: legacy.signal,
                                code: legacy.code,
                                name: legacy.name,
                                code_name: legacy.code_name,
                            }),
                            mach_exception: legacy.mach_exception.map_value(|legacy| {
                                MachException {
                                    ty: legacy.exception,
                                    code: legacy.code,
                                    subcode: legacy.subcode,
                                    name: legacy.exception_name,
                                }
                            }),
                            other: Object::default(),
                        }),
                        other: Object::default(),
                    })
                }
            }
            Annotated(Some(Value::Null), meta) => Annotated(None, meta),
            Annotated(Some(value), mut meta) => {
                meta.add_unexpected_value_error("exception mechanism", value);
                Annotated(None, meta)
            }
            Annotated(None, meta) => Annotated(None, meta),
        }
    }
}

fn get_errno_name(errno: i64, os_hint: OsHint) -> Option<&'static str> {
    Some(match os_hint {
        OsHint::Linux => match errno {
            1 => "EPERM",    // Operation not permitted
            2 => "ENOENT",   // No such file or directory
            3 => "ESRCH",    // No such process
            4 => "EINTR",    // Interrupted system call
            5 => "EIO",      // I/O error
            6 => "ENXIO",    // No such device or address
            7 => "E2BIG",    // Argument list too long
            8 => "ENOEXEC",  // Exec format error
            9 => "EBADF",    // Bad file number
            10 => "ECHILD",  // No child processes
            11 => "EAGAIN",  // Try again
            12 => "ENOMEM",  // Out of memory
            13 => "EACCES",  // Permission denied
            14 => "EFAULT",  // Bad address
            15 => "ENOTBLK", // Block device required
            16 => "EBUSY",   // Device or resource busy
            17 => "EEXIST",  // File exists
            18 => "EXDEV",   // Cross-device link
            19 => "ENODEV",  // No such device
            20 => "ENOTDIR", // Not a directory
            21 => "EISDIR",  // Is a directory
            22 => "EINVAL",  // Invalid argument
            23 => "ENFILE",  // File table overflow
            24 => "EMFILE",  // Too many open files
            25 => "ENOTTY",  // Not a typewriter
            26 => "ETXTBSY", // Text file busy
            27 => "EFBIG",   // File too large
            28 => "ENOSPC",  // No space left on device
            29 => "ESPIPE",  // Illegal seek
            30 => "EROFS",   // Read-only file system
            31 => "EMLINK",  // Too many links
            32 => "EPIPE",   // Broken pipe
            33 => "EDOM",    // Math argument out of domain of func
            34 => "ERANGE",  // Math result not representable

            35 => "EDEADLK",      // Resource deadlock would occur
            36 => "ENAMETOOLONG", // File name too long
            37 => "ENOLCK",       // No record locks available

            38 => "ENOSYS", // Invalid system call number

            39 => "ENOTEMPTY", // Directory not empty
            40 => "ELOOP",     // Too many symbolic links encountered
            42 => "ENOMSG",    // No message of desired type
            43 => "EIDRM",     // Identifier removed
            44 => "ECHRNG",    // Channel number out of range
            45 => "EL2NSYNC",  // Level 2 not synchronized
            46 => "EL3HLT",    // Level 3 halted
            47 => "EL3RST",    // Level 3 reset
            48 => "ELNRNG",    // Link number out of range
            49 => "EUNATCH",   // Protocol driver not attached
            50 => "ENOCSI",    // No CSI structure available
            51 => "EL2HLT",    // Level 2 halted
            52 => "EBADE",     // Invalid exchange
            53 => "EBADR",     // Invalid request descriptor
            54 => "EXFULL",    // Exchange full
            55 => "ENOANO",    // No anode
            56 => "EBADRQC",   // Invalid request code
            57 => "EBADSLT",   // Invalid slot

            59 => "EBFONT",          // Bad font file format
            60 => "ENOSTR",          // Device not a stream
            61 => "ENODATA",         // No data available
            62 => "ETIME",           // Timer expired
            63 => "ENOSR",           // Out of streams resources
            64 => "ENONET",          // Machine is not on the network
            65 => "ENOPKG",          // Package not installed
            66 => "EREMOTE",         // Object is remote
            67 => "ENOLINK",         // Link has been severed
            68 => "EADV",            // Advertise error
            69 => "ESRMNT",          // Srmount error
            70 => "ECOMM",           // Communication error on send
            71 => "EPROTO",          // Protocol error
            72 => "EMULTIHOP",       // Multihop attempted
            73 => "EDOTDOT",         // RFS specific error
            74 => "EBADMSG",         // Not a data message
            75 => "EOVERFLOW",       // Value too large for defined data type
            76 => "ENOTUNIQ",        // Name not unique on network
            77 => "EBADFD",          // File descriptor in bad state
            78 => "EREMCHG",         // Remote address changed
            79 => "ELIBACC",         // Can not access a needed shared library
            80 => "ELIBBAD",         // Accessing a corrupted shared library
            81 => "ELIBSCN",         // .lib section in a.out corrupted
            82 => "ELIBMAX",         // Attempting to link in too many shared libraries
            83 => "ELIBEXEC",        // Cannot exec a shared library directly
            84 => "EILSEQ",          // Illegal byte sequence
            85 => "ERESTART",        // Interrupted system call should be restarted
            86 => "ESTRPIPE",        // Streams pipe error
            87 => "EUSERS",          // Too many users
            88 => "ENOTSOCK",        // Socket operation on non-socket
            89 => "EDESTADDRREQ",    // Destination address required
            90 => "EMSGSIZE",        // Message too long
            91 => "EPROTOTYPE",      // Protocol wrong type for socket
            92 => "ENOPROTOOPT",     // Protocol not available
            93 => "EPROTONOSUPPORT", // Protocol not supported
            94 => "ESOCKTNOSUPPORT", // Socket type not supported
            95 => "EOPNOTSUPP",      // Operation not supported on transport endpoint
            96 => "EPFNOSUPPORT",    // Protocol family not supported
            97 => "EAFNOSUPPORT",    // Address family not supported by protocol
            98 => "EADDRINUSE",      // Address already in use
            99 => "EADDRNOTAVAIL",   // Cannot assign requested address
            100 => "ENETDOWN",       // Network is down
            101 => "ENETUNREACH",    // Network is unreachable
            102 => "ENETRESET",      // Network dropped connection because of reset
            103 => "ECONNABORTED",   // Software caused connection abort
            104 => "ECONNRESET",     // Connection reset by peer
            105 => "ENOBUFS",        // No buffer space available
            106 => "EISCONN",        // Transport endpoint is already connected
            107 => "ENOTCONN",       // Transport endpoint is not connected
            108 => "ESHUTDOWN",      // Cannot send after transport endpoint shutdown
            109 => "ETOOMANYREFS",   // Too many references: cannot splice
            110 => "ETIMEDOUT",      // Connection timed out
            111 => "ECONNREFUSED",   // Connection refused
            112 => "EHOSTDOWN",      // Host is down
            113 => "EHOSTUNREACH",   // No route to host
            114 => "EALREADY",       // Operation already in progress
            115 => "EINPROGRESS",    // Operation now in progress
            116 => "ESTALE",         // Stale file handle
            117 => "EUCLEAN",        // Structure needs cleaning
            118 => "ENOTNAM",        // Not a XENIX named type file
            119 => "ENAVAIL",        // No XENIX semaphores available
            120 => "EISNAM",         // Is a named type file
            121 => "EREMOTEIO",      // Remote I/O error
            122 => "EDQUOT",         // Quota exceeded

            123 => "ENOMEDIUM",    // No medium found
            124 => "EMEDIUMTYPE",  // Wrong medium type
            125 => "ECANCELED",    // Operation Canceled
            126 => "ENOKEY",       // Required key not available
            127 => "EKEYEXPIRED",  // Key has expired
            128 => "EKEYREVOKED",  // Key has been revoked
            129 => "EKEYREJECTED", // Key was rejected by service

            130 => "EOWNERDEAD",      // Owner died
            131 => "ENOTRECOVERABLE", // State not recoverable

            132 => "ERFKILL", // Operation not possible due to RF-kill

            133 => "EHWPOISON", // Memory page has hardware error
            _ => return None,
        },
        OsHint::Darwin => match errno {
            1 => "EPERM",    // Operation not permitted
            2 => "ENOENT",   // No such file or directory
            3 => "ESRCH",    // No such process
            4 => "EINTR",    // Interrupted system call
            5 => "EIO",      // Input/output error
            6 => "ENXIO",    // Device not configured
            7 => "E2BIG",    // Argument list too long
            8 => "ENOEXEC",  // Exec format error
            9 => "EBADF",    // Bad file descriptor
            10 => "ECHILD",  // No child processes
            11 => "EDEADLK", // Resource deadlock avoided
            12 => "ENOMEM",  // Cannot allocate memory
            13 => "EACCES",  // Permission denied
            14 => "EFAULT",  // Bad address
            15 => "ENOTBLK", // Block device required
            16 => "EBUSY",   // Device / Resource busy
            17 => "EEXIST",  // File exists
            18 => "EXDEV",   // Cross-device link
            19 => "ENODEV",  // Operation not supported by device
            20 => "ENOTDIR", // Not a directory
            21 => "EISDIR",  // Is a directory
            22 => "EINVAL",  // Invalid argument
            23 => "ENFILE",  // Too many open files in system
            24 => "EMFILE",  // Too many open files
            25 => "ENOTTY",  // Inappropriate ioctl for device
            26 => "ETXTBSY", // Text file busy
            27 => "EFBIG",   // File too large
            28 => "ENOSPC",  // No space left on device
            29 => "ESPIPE",  // Illegal seek
            30 => "EROFS",   // Read-only file system
            31 => "EMLINK",  // Too many links
            32 => "EPIPE",   // Broken pipe

            // math software
            33 => "EDOM",   // Numerical argument out of domain
            34 => "ERANGE", // Result too large

            // non - blocking and interrupt i / o
            35 => "EAGAIN",      // Resource temporarily unavailable
            36 => "EINPROGRESS", // Operation now in progress
            37 => "EALREADY",    // Operation already in progress

            // ipc / network software - - argument errors
            38 => "ENOTSOCK",        // Socket operation on non-socket
            39 => "EDESTADDRREQ",    // Destination address required
            40 => "EMSGSIZE",        // Message too long
            41 => "EPROTOTYPE",      // Protocol wrong type for socket
            42 => "ENOPROTOOPT",     // Protocol not available
            43 => "EPROTONOSUPPORT", // Protocol not supported
            44 => "ESOCKTNOSUPPORT", // Socket type not supported
            45 => "ENOTSUP",         // Operation not supported

            46 => "EPFNOSUPPORT",  // Protocol family not supported
            47 => "EAFNOSUPPORT",  // Address family not supported by protocol family
            48 => "EADDRINUSE",    // Address already in use
            49 => "EADDRNOTAVAIL", // Can"t assign requested address

            // ipc / network software - - operational errors
            50 => "ENETDOWN",     // Network is down
            51 => "ENETUNREACH",  // Network is unreachable
            52 => "ENETRESET",    // Network dropped connection on reset
            53 => "ECONNABORTED", // Software caused connection abort
            54 => "ECONNRESET",   // Connection reset by peer
            55 => "ENOBUFS",      // No buffer space available
            56 => "EISCONN",      // Socket is already connected
            57 => "ENOTCONN",     // Socket is not connected
            58 => "ESHUTDOWN",    // Can"t send after socket shutdown
            59 => "ETOOMANYREFS", // Too many references: can"t splice
            60 => "ETIMEDOUT",    // Operation timed out
            61 => "ECONNREFUSED", // Connection refused

            62 => "ELOOP",        // Too many levels of symbolic links
            63 => "ENAMETOOLONG", // File name too long

            // should be rearranged
            64 => "EHOSTDOWN",    // Host is down
            65 => "EHOSTUNREACH", // No route to host
            66 => "ENOTEMPTY",    // Directory not empty

            // quotas & mush
            67 => "EPROCLIM", // Too many processes
            68 => "EUSERS",   // Too many users
            69 => "EDQUOT",   // Disc quota exceeded

            // Network File System
            70 => "ESTALE",        // Stale NFS file handle
            71 => "EREMOTE",       // Too many levels of remote in path
            72 => "EBADRPC",       // RPC struct is bad
            73 => "ERPCMISMATCH",  // RPC version wrong
            74 => "EPROGUNAVAIL",  // RPC prog. not avail
            75 => "EPROGMISMATCH", // Program version wrong
            76 => "EPROCUNAVAIL",  // Bad procedure for program

            77 => "ENOLCK", // No locks available
            78 => "ENOSYS", // Function not implemented

            79 => "EFTYPE",    // Inappropriate file type or format
            80 => "EAUTH",     // Authentication error
            81 => "ENEEDAUTH", // Need authenticator

            // Intelligent device errors
            82 => "EPWROFF", // Device power is off
            83 => "EDEVERR", // Device error, e.g. paper out

            84 => "EOVERFLOW", // Value too large to be stored in data type

            // Program loading errors
            85 => "EBADEXEC",   // Bad executable
            86 => "EBADARCH",   // Bad CPU type in executable
            87 => "ESHLIBVERS", // Shared library version mismatch
            88 => "EBADMACHO",  // Malformed Macho file

            89 => "ECANCELED", // Operation canceled

            90 => "EIDRM",   // Identifier removed
            91 => "ENOMSG",  // No message of desired type
            92 => "EILSEQ",  // Illegal byte sequence
            93 => "ENOATTR", // Attribute not found

            94 => "EBADMSG",   // Bad message
            95 => "EMULTIHOP", // Reserved
            96 => "ENODATA",   // No message available on STREAM
            97 => "ENOLINK",   // Reserved
            98 => "ENOSR",     // No STREAM resources
            99 => "ENOSTR",    // Not a STREAM
            100 => "EPROTO",   // Protocol error
            101 => "ETIME",    // STREAM ioctl timeout

            102 => "EOPNOTSUPP",      // Operation not supported on socket
            103 => "ENOPOLICY",       // No such policy registered
            104 => "ENOTRECOVERABLE", // State not recoverable
            105 => "EOWNERDEAD",      // Previous owner died
            106 => "EQFULL",          // Interface output queue is full
            _ => return None,
        },
        OsHint::Windows => match errno {
            1 => "EPERM",
            2 => "ENOENT",
            3 => "ESRCH",
            4 => "EINTR",
            5 => "EIO",
            6 => "ENXIO",
            7 => "E2BIG",
            8 => "ENOEXEC",
            9 => "EBADF",
            10 => "ECHILD",
            11 => "EAGAIN",
            12 => "ENOMEM",
            13 => "EACCES",
            14 => "EFAULT",
            16 => "EBUSY",
            17 => "EEXIST",
            18 => "EXDEV",
            19 => "ENODEV",
            20 => "ENOTDIR",
            21 => "EISDIR",
            23 => "ENFILE",
            24 => "EMFILE",
            25 => "ENOTTY",
            27 => "EFBIG",
            28 => "ENOSPC",
            29 => "ESPIPE",
            30 => "EROFS",
            31 => "EMLINK",
            32 => "EPIPE",
            33 => "EDOM",
            36 => "EDEADLK",
            38 => "ENAMETOOLONG",
            39 => "ENOLCK",
            40 => "ENOSYS",
            41 => "ENOTEMPTY",

            // Error codes used in the Secure CRT functions
            22 => "EINVAL",
            34 => "ERANGE",
            42 => "EILSEQ",
            80 => "STRUNCATE",

            // POSIX Supplement
            100 => "EADDRINUSE",
            101 => "EADDRNOTAVAIL",
            102 => "EAFNOSUPPORT",
            103 => "EALREADY",
            104 => "EBADMSG",
            105 => "ECANCELED",
            106 => "ECONNABORTED",
            107 => "ECONNREFUSED",
            108 => "ECONNRESET",
            109 => "EDESTADDRREQ",
            110 => "EHOSTUNREACH",
            111 => "EIDRM",
            112 => "EINPROGRESS",
            113 => "EISCONN",
            114 => "ELOOP",
            115 => "EMSGSIZE",
            116 => "ENETDOWN",
            117 => "ENETRESET",
            118 => "ENETUNREACH",
            119 => "ENOBUFS",
            120 => "ENODATA",
            121 => "ENOLINK",
            122 => "ENOMSG",
            123 => "ENOPROTOOPT",
            124 => "ENOSR",
            125 => "ENOSTR",
            126 => "ENOTCONN",
            127 => "ENOTRECOVERABLE",
            128 => "ENOTSOCK",
            129 => "ENOTSUP",
            130 => "EOPNOTSUPP",
            131 => "EOTHER",
            132 => "EOVERFLOW",
            133 => "EOWNERDEAD",
            134 => "EPROTO",
            135 => "EPROTONOSUPPORT",
            136 => "EPROTOTYPE",
            137 => "ETIME",
            138 => "ETIMEDOUT",
            139 => "ETXTBSY",
            140 => "EWOULDBLOCK",
            _ => return None,
        },
    })
}

fn get_signal_name(signo: i64, os_hint: OsHint) -> Option<&'static str> {
    // Linux signals have been taken from <uapi/asm-generic/signal.h>
    Some(match os_hint {
        OsHint::Linux => match signo {
            1 => "SIGHUP",  // Hangup.
            2 => "SIGINT",  // Terminal interrupt signal.
            3 => "SIGQUIT", // Terminal quit signal.
            4 => "SIGILL",  // Illegal instruction.
            5 => "SIGTRAP",
            6 => "SIGABRT", // Process abort signal.
            7 => "SIGBUS",
            8 => "SIGFPE",   // Erroneous arithmetic operation.
            9 => "SIGKILL",  // Kill (cannot be caught or ignored).
            10 => "SIGUSR1", // User-defined signal 1.
            11 => "SIGSEGV", // Invalid memory reference.
            12 => "SIGUSR2", // User-defined signal 2.
            13 => "SIGPIPE", // Write on a pipe with no one to read it.
            14 => "SIGALRM", // Alarm clock.
            15 => "SIGTERM", // Termination signal.
            16 => "SIGSTKFLT",
            17 => "SIGCHLD",   // Child process terminated or stopped.
            18 => "SIGCONT",   // Continue executing, if stopped.
            19 => "SIGSTOP",   // Stop executing (cannot be caught or ignored).
            20 => "SIGTSTP",   // Terminal stop signal.
            21 => "SIGTTIN",   // Background process attempting read.
            22 => "SIGTTOU",   // Background process attempting write.
            23 => "SIGURG",    // High bandwidth data is available at a socket.
            24 => "SIGXCPU",   // CPU time limit exceeded.
            25 => "SIGXFSZ",   // File size limit exceeded.
            26 => "SIGVTALRM", // Virtual timer expired.
            27 => "SIGPROF",   // Profiling timer expired.
            28 => "SIGWINCH",
            29 => "SIGIO",
            30 => "SIGPWR",
            31 => "SIGSYS",
            _ => return None,
        },
        OsHint::Darwin => match signo {
            1 => "SIGHUP",  // hangup
            2 => "SIGINT",  // interrupt
            3 => "SIGQUIT", // quit
            4 => "SIGILL",  // illegal instruction (not reset when caught)
            5 => "SIGTRAP", // trace trap (not reset when caught)
            6 => "SIGABRT", // abort()
            // if (defined(_POSIX_C_SOURCE) && !defined(_DARWIN_C_SOURCE))
            7 => "SIGPOLL", // pollable event ([XSR] generated, not supported)
            // if (!_POSIX_C_SOURCE || _DARWIN_C_SOURCE)
            // 7 => "SIGEMT", // EMT instruction
            8 => "SIGFPE",     // floating point exception
            9 => "SIGKILL",    // kill (cannot be caught or ignored)
            10 => "SIGBUS",    // bus error
            11 => "SIGSEGV",   // segmentation violation
            12 => "SIGSYS",    // bad argument to system call
            13 => "SIGPIPE",   // write on a pipe with no one to read it
            14 => "SIGALRM",   // alarm clock
            15 => "SIGTERM",   // software termination signal from kill
            16 => "SIGURG",    // urgent condition on IO channel
            17 => "SIGSTOP",   // sendable stop signal not from tty
            18 => "SIGTSTP",   // stop signal from tty
            19 => "SIGCONT",   // continue a stopped process
            20 => "SIGCHLD",   // to parent on child stop or exit
            21 => "SIGTTIN",   // to readers pgrp upon background tty read
            22 => "SIGTTOU",   // like TTIN for output if (tp->t_local&LTOSTOP)
            23 => "SIGIO",     // input/output possible signal
            24 => "SIGXCPU",   // exceeded CPU time limit
            25 => "SIGXFSZ",   // exceeded file size limit
            26 => "SIGVTALRM", // virtual time alarm
            27 => "SIGPROF",   // profiling time alarm
            28 => "SIGWINCH",  // window size changes
            29 => "SIGINFO",   // information request
            30 => "SIGUSR1",   // user defined signal 1
            31 => "SIGUSR2",   // user defined signal 2
            _ => return None,
        },
        _ => return None,
    })
}

fn get_signal_code_name(signo: i64, codeno: i64) -> Option<&'static str> {
    // Codes for Darwin `si_code`
    Some(match signo {
        // Codes for SIGILL
        4 => match codeno {
            0 => "ILL_NOOP",   // if only I knew...
            1 => "ILL_ILLOPC", // [XSI] illegal opcode
            2 => "ILL_ILLTRP", // [XSI] illegal trap
            3 => "ILL_PRVOPC", // [XSI] privileged opcode
            4 => "ILL_ILLOPN", // [XSI] illegal operand -NOTIMP
            5 => "ILL_ILLADR", // [XSI] illegal addressing mode -NOTIMP
            6 => "ILL_PRVREG", // [XSI] privileged register -NOTIMP
            7 => "ILL_COPROC", // [XSI] coprocessor error -NOTIMP
            8 => "ILL_BADSTK", // [XSI] internal stack error -NOTIMP
            _ => return None,
        },

        // Codes for SIGFPE
        8 => match codeno {
            0 => "FPE_NOOP",   // if only I knew...
            1 => "FPE_FLTDIV", // [XSI] floating point divide by zero
            2 => "FPE_FLTOVF", // [XSI] floating point overflow
            3 => "FPE_FLTUND", // [XSI] floating point underflow
            4 => "FPE_FLTRES", // [XSI] floating point inexact result
            5 => "FPE_FLTINV", // [XSI] invalid floating point operation
            6 => "FPE_FLTSUB", // [XSI] subscript out of range -NOTIMP
            7 => "FPE_INTDIV", // [XSI] integer divide by zero
            8 => "FPE_INTOVF", // [XSI] integer overflow
            _ => return None,
        },

        // Codes for SIGSEGV
        11 => match codeno {
            0 => "SEGV_NOOP",   // if only I knew...
            1 => "SEGV_MAPERR", // [XSI] address not mapped to object
            2 => "SEGV_ACCERR", // [XSI] invalid permission for mapped object
            _ => return None,
        },

        // Codes for SIGBUS
        10 => match codeno {
            0 => "BUS_NOOP",   // if only I knew...
            1 => "BUS_ADRALN", // [XSI] Invalid address alignment
            2 => "BUS_ADRERR", // [XSI] Nonexistent physical address -NOTIMP
            3 => "BUS_OBJERR", // [XSI] Object-specific HW error - NOTIMP
            _ => return None,
        },

        // Codes for SIGTRAP
        5 => match codeno {
            1 => "TRAP_BRKPT", // [XSI] Process breakpoint -NOTIMP
            2 => "TRAP_TRACE", // [XSI] Process trace trap -NOTIMP
            _ => return None,
        },

        // Codes for SIGCHLD
        20 => match codeno {
            0 => "CLD_NOOP",      // if only I knew...
            1 => "CLD_EXITED",    // [XSI] child has exited
            2 => "CLD_KILLED",    // [XSI] terminated abnormally, no core file
            3 => "CLD_DUMPED",    // [XSI] terminated abnormally, core file
            4 => "CLD_TRAPPED",   // [XSI] traced child has trapped
            5 => "CLD_STOPPED",   // [XSI] child has stopped
            6 => "CLD_CONTINUED", // [XSI] stopped child has continued
            _ => return None,
        },

        // Codes for SIGPOLL
        7 => match codeno {
            1 => "POLL_IN",  // [XSR] Data input available
            2 => "POLL_OUT", // [XSR] Output buffers available
            3 => "POLL_MSG", // [XSR] Input message available
            4 => "POLL_ERR", // [XSR] I/O error
            5 => "POLL_PRI", // [XSR] High priority input available
            6 => "POLL_HUP", // [XSR] Device disconnected
            _ => return None,
        },
        _ => return None,
    })
}

fn get_mach_exception_name(number: i64) -> Option<&'static str> {
    // Mach exception codes used in Darwin.
    Some(match number {
        1 => "EXC_BAD_ACCESS",      // Could not access memory
        2 => "EXC_BAD_INSTRUCTION", // Instruction failed
        3 => "EXC_ARITHMETIC",      // Arithmetic exception
        4 => "EXC_EMULATION",       // Emulation instruction
        5 => "EXC_SOFTWARE",        // Software generated exception
        6 => "EXC_BREAKPOINT",      // Trace, breakpoint, etc.
        7 => "EXC_SYSCALL",         // System calls.
        8 => "EXC_MACH_SYSCALL",    // Mach system calls.
        9 => "EXC_RPC_ALERT",       // RPC alert
        10 => "EXC_CRASH",          // Abnormal process exit
        11 => "EXC_RESOURCE",       // Hit resource consumption limit
        12 => "EXC_GUARD",          // Violated guarded resource protections
        13 => "EXC_CORPSE_NOTIFY",  // Abnormal process exited to corpse state
        _ => return None,
    })
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub(crate) enum OsHint {
    Windows,
    Linux,
    Darwin,
}

impl OsHint {
    pub fn from_event(event: &Event) -> Option<OsHint> {
        if let Some(ref debug_meta) = event.debug_meta.0 {
            if let Some(ref sdk_info) = debug_meta.system_sdk.0 {
                return normalize_sdk_name(&sdk_info.sdk_name.0);
            }
        }

        if let Some(ref contexts) = event.contexts.0 {
            if let Some(&Annotated(Some(Context::Os(ref os_context)), _)) = contexts.0.get("os") {
                return normalize_sdk_name(&os_context.name.0);
            }
        }

        None
    }
}

fn normalize_sdk_name(name: &Option<String>) -> Option<OsHint> {
    if let Some(ref name) = name {
        match &**name {
            "ios" | "watchos" | "tvos" | "macos" => Some(OsHint::Darwin),
            "linux" | "android" => Some(OsHint::Linux),
            "windows" => Some(OsHint::Windows),
            _ => None,
        }
    } else {
        None
    }
}

pub(crate) fn normalize_mechanism_meta(mechanism: &mut Mechanism, os_hint: Option<OsHint>) {
    if let Some(ref mut meta) = mechanism.meta.0 {
        if let Some(os_hint) = os_hint {
            if let Some(ref mut cerror) = meta.errno.0 {
                if cerror.name.0.is_none() {
                    if let Some(name) = cerror.number.0.and_then(|x| get_errno_name(x, os_hint)) {
                        cerror.name = Annotated::new(name.to_owned());
                    }
                }
            }

            if let Some(ref mut signal) = meta.signal.0 {
                if let Some(signo) = signal.number.0 {
                    if signal.name.0.is_none() {
                        if let Some(name) = get_signal_name(signo, os_hint) {
                            signal.name = Annotated::new(name.to_owned());
                        }
                    }

                    if os_hint == OsHint::Darwin && signal.code_name.0.is_none() {
                        if let Some(code_name) =
                            signal.code.0.and_then(|x| get_signal_code_name(signo, x))
                        {
                            signal.code_name = Annotated::new(code_name.to_owned());
                        }
                    }
                }
            }
        }

        if let Some(ref mut mach_exception) = meta.mach_exception.0 {
            if let Some(number) = mach_exception.ty.0 {
                if mach_exception.name.0.is_none() {
                    if let Some(name) = get_mach_exception_name(number) {
                        mach_exception.name = Annotated::new(name.to_owned());
                    }
                }
            }
        }
    }
}

#[test]
fn test_mechanism_roundtrip() {
    let json = r#"{
  "type": "mytype",
  "description": "mydescription",
  "help_link": "https://developer.apple.com/library/content/qa/qa1367/_index.html",
  "handled": false,
  "data": {
    "relevant_address": "0x1"
  },
  "meta": {
    "errno": {
      "number": 2,
      "name": "ENOENT"
    },
    "signal": {
      "number": 11,
      "code": 0,
      "name": "SIGSEGV",
      "code_name": "SEGV_NOOP"
    },
    "mach_exception": {
      "exception": 1,
      "code": 1,
      "subcode": 8,
      "name": "EXC_BAD_ACCESS"
    },
    "other": "value"
  },
  "other": "value"
}"#;
    let mechanism = Annotated::new(Mechanism {
        ty: Annotated::new("mytype".to_string()),
        description: Annotated::new("mydescription".to_string()),
        help_link: Annotated::new(
            "https://developer.apple.com/library/content/qa/qa1367/_index.html".to_string(),
        ),
        handled: Annotated::new(false),
        data: {
            let mut map = Map::new();
            map.insert(
                "relevant_address".to_string(),
                Annotated::new(Value::String("0x1".to_string())),
            );
            Annotated::new(map)
        },
        meta: Annotated::new(MechanismMeta {
            errno: Annotated::new(CError {
                number: Annotated::new(2),
                name: Annotated::new("ENOENT".to_string()),
            }),
            mach_exception: Annotated::new(MachException {
                ty: Annotated::new(1),
                code: Annotated::new(1),
                subcode: Annotated::new(8),
                name: Annotated::new("EXC_BAD_ACCESS".to_string()),
            }),
            signal: Annotated::new(PosixSignal {
                number: Annotated::new(11),
                code: Annotated::new(0),
                name: Annotated::new("SIGSEGV".to_string()),
                code_name: Annotated::new("SEGV_NOOP".to_string()),
            }),
            other: {
                let mut map = Object::new();
                map.insert(
                    "other".to_string(),
                    Annotated::new(Value::String("value".to_string())),
                );
                map
            },
        }),
        other: {
            let mut map = Object::new();
            map.insert(
                "other".to_string(),
                Annotated::new(Value::String("value".to_string())),
            );
            map
        },
    });

    assert_eq_dbg!(mechanism, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, mechanism.to_json_pretty().unwrap());
}

#[test]
fn test_mechanism_default_values() {
    let json = r#"{"type":"mytype"}"#;
    let mechanism = Annotated::new(Mechanism {
        ty: Annotated::new("mytype".to_string()),
        ..Default::default()
    });

    assert_eq_dbg!(mechanism, Annotated::from_json(json).unwrap());
    assert_eq_str!(json, mechanism.to_json().unwrap());
}

#[test]
fn test_mechanism_empty() {
    let mechanism = Annotated::<Mechanism>::empty();
    assert_eq_dbg!(mechanism, Annotated::from_json("{}").unwrap());
}

#[test]
fn test_mechanism_invalid_meta() {
    let json = r#"{
  "type":"mytype",
  "meta": {
    "errno": {"name": "ENOENT"},
    "mach_exception": {"name": "EXC_BAD_ACCESS"},
    "signal": {"name": "SIGSEGV"}
  }
}"#;
    let mechanism = Annotated::new(Mechanism {
        ty: Annotated::new("mytype".to_string()),
        meta: Annotated::new(MechanismMeta {
            errno: Annotated::new(CError {
                number: Annotated::from_error("value required", None),
                name: Annotated::new("ENOENT".to_string()),
            }),
            mach_exception: Annotated::new(MachException {
                ty: Annotated::from_error("value required", None),
                code: Annotated::from_error("value required", None),
                subcode: Annotated::from_error("value required", None),
                name: Annotated::new("EXC_BAD_ACCESS".to_string()),
            }),
            signal: Annotated::new(PosixSignal {
                number: Annotated::from_error("value required", None),
                code: Annotated::empty(),
                name: Annotated::new("SIGSEGV".to_string()),
                code_name: Annotated::empty(),
            }),
            ..Default::default()
        }),
        ..Default::default()
    });

    assert_eq_dbg!(mechanism, Annotated::from_json(json).unwrap());
}

#[test]
fn test_mechanism_legacy_conversion() {
    let input = r#"{
  "posix_signal": {
    "name": "SIGSEGV",
    "code_name": "SEGV_NOOP",
    "signal": 11,
    "code": 0
  },
  "relevant_address": "0x1",
  "mach_exception": {
    "exception": 1,
    "exception_name": "EXC_BAD_ACCESS",
    "subcode": 8,
    "code": 1
  }
}"#;

    let output = r#"{
  "type": "generic",
  "data": {
    "relevant_address": "0x1"
  },
  "meta": {
    "signal": {
      "number": 11,
      "code": 0,
      "name": "SIGSEGV",
      "code_name": "SEGV_NOOP"
    },
    "mach_exception": {
      "exception": 1,
      "code": 1,
      "subcode": 8,
      "name": "EXC_BAD_ACCESS"
    }
  }
}"#;
    let mechanism = Annotated::new(Mechanism {
        ty: Annotated::new("generic".to_string()),
        description: Annotated::empty(),
        help_link: Annotated::empty(),
        handled: Annotated::empty(),
        data: {
            let mut map = Map::new();
            map.insert(
                "relevant_address".to_string(),
                Annotated::new(Value::String("0x1".to_string())),
            );
            Annotated::new(map)
        },
        meta: Annotated::new(MechanismMeta {
            errno: Annotated::empty(),
            mach_exception: Annotated::new(MachException {
                ty: Annotated::new(1),
                code: Annotated::new(1),
                subcode: Annotated::new(8),
                name: Annotated::new("EXC_BAD_ACCESS".to_string()),
            }),
            signal: Annotated::new(PosixSignal {
                number: Annotated::new(11),
                code: Annotated::new(0),
                name: Annotated::new("SIGSEGV".to_string()),
                code_name: Annotated::new("SEGV_NOOP".to_string()),
            }),
            other: Object::default(),
        }),
        other: Object::default(),
    });

    assert_eq_dbg!(mechanism, Annotated::from_json(input).unwrap());
    assert_eq_str!(output, mechanism.to_json_pretty().unwrap());
}

#[test]
fn test_normalize_missing() {
    let mut mechanism = Mechanism {
        ty: Annotated::new("generic".to_string()),
        ..Default::default()
    };

    let old_mechanism = mechanism.clone();

    normalize_mechanism_meta(&mut mechanism, None);

    assert_eq!(mechanism, old_mechanism);
}

#[test]
fn test_normalize_errno() {
    let mut mechanism = Mechanism {
        ty: Annotated::new("generic".to_string()),
        meta: Annotated::new(MechanismMeta {
            errno: Annotated::new(CError {
                number: Annotated::new(2),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    normalize_mechanism_meta(&mut mechanism, Some(OsHint::Linux));

    let errno = mechanism.meta.0.unwrap().errno.0.unwrap();
    assert_eq!(
        errno,
        CError {
            number: Annotated::new(2),
            name: Annotated::new("ENOENT".to_string()),
        }
    );
}

#[test]
fn test_normalize_errno_override() {
    let mut mechanism = Mechanism {
        ty: Annotated::new("generic".to_string()),
        meta: Annotated::new(MechanismMeta {
            errno: Annotated::new(CError {
                number: Annotated::new(2),
                name: Annotated::new("OVERRIDDEN".to_string()),
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    normalize_mechanism_meta(&mut mechanism, Some(OsHint::Linux));

    let errno = mechanism.meta.0.unwrap().errno.0.unwrap();
    assert_eq!(
        errno,
        CError {
            number: Annotated::new(2),
            name: Annotated::new("OVERRIDDEN".to_string()),
        }
    );
}

#[test]
fn test_normalize_errno_fail() {
    let mut mechanism = Mechanism {
        ty: Annotated::new("generic".to_string()),
        meta: Annotated::new(MechanismMeta {
            errno: Annotated::new(CError {
                number: Annotated::new(2),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    normalize_mechanism_meta(&mut mechanism, None);

    let errno = mechanism.meta.0.unwrap().errno.0.unwrap();
    assert_eq!(
        errno,
        CError {
            number: Annotated::new(2),
            ..Default::default()
        }
    );
}

#[test]
fn test_normalize_signal() {
    let mut mechanism = Mechanism {
        ty: Annotated::new("generic".to_string()),
        meta: Annotated::new(MechanismMeta {
            signal: Annotated::new(PosixSignal {
                number: Annotated::new(11),
                code: Annotated::new(0),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    normalize_mechanism_meta(&mut mechanism, Some(OsHint::Darwin));

    let signal = mechanism.meta.0.unwrap().signal.0.unwrap();
    assert_eq!(
        signal,
        PosixSignal {
            number: Annotated::new(11),
            code: Annotated::new(0),
            name: Annotated::new("SIGSEGV".to_string()),
            code_name: Annotated::new("SEGV_NOOP".to_string()),
        }
    );
}

#[test]
fn test_normalize_partial_signal() {
    let mut mechanism = Mechanism {
        ty: Annotated::new("generic".to_string()),
        meta: Annotated::new(MechanismMeta {
            signal: Annotated::new(PosixSignal {
                number: Annotated::new(11),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    normalize_mechanism_meta(&mut mechanism, Some(OsHint::Linux));

    let signal = mechanism.meta.0.unwrap().signal.0.unwrap();

    assert_eq!(
        signal,
        PosixSignal {
            number: Annotated::new(11),
            name: Annotated::new("SIGSEGV".to_string()),
            ..Default::default()
        }
    );
}

#[test]
fn test_normalize_signal_override() {
    let mut mechanism = Mechanism {
        ty: Annotated::new("generic".to_string()),
        meta: Annotated::new(MechanismMeta {
            signal: Annotated::new(PosixSignal {
                number: Annotated::new(11),
                code: Annotated::new(0),
                name: Annotated::new("OVERRIDDEN".to_string()),
                code_name: Annotated::new("OVERRIDDEN".to_string()),
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    normalize_mechanism_meta(&mut mechanism, Some(OsHint::Linux));

    let signal = mechanism.meta.0.unwrap().signal.0.unwrap();

    assert_eq!(
        signal,
        PosixSignal {
            number: Annotated::new(11),
            code: Annotated::new(0),
            name: Annotated::new("OVERRIDDEN".to_string()),
            code_name: Annotated::new("OVERRIDDEN".to_string()),
        }
    );
}

#[test]
fn test_normalize_signal_fail() {
    let mut mechanism = Mechanism {
        ty: Annotated::new("generic".to_string()),
        meta: Annotated::new(MechanismMeta {
            signal: Annotated::new(PosixSignal {
                number: Annotated::new(11),
                code: Annotated::new(0),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    normalize_mechanism_meta(&mut mechanism, None);

    let signal = mechanism.meta.0.unwrap().signal.0.unwrap();

    assert_eq!(
        signal,
        PosixSignal {
            number: Annotated::new(11),
            code: Annotated::new(0),
            ..Default::default()
        }
    );
}

#[test]
fn test_normalize_mach() {
    let mut mechanism = Mechanism {
        ty: Annotated::new("generic".to_string()),
        meta: Annotated::new(MechanismMeta {
            mach_exception: Annotated::new(MachException {
                ty: Annotated::new(1),
                code: Annotated::new(1),
                subcode: Annotated::new(8),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    // We do not need SDK information here because mach exceptions only
    // occur on Darwin

    normalize_mechanism_meta(&mut mechanism, None);

    let mach_exception = mechanism.meta.0.unwrap().mach_exception.0.unwrap();
    assert_eq!(
        mach_exception,
        MachException {
            ty: Annotated::new(1),
            code: Annotated::new(1),
            subcode: Annotated::new(8),
            name: Annotated::new("EXC_BAD_ACCESS".to_string()),
        }
    );
}

#[test]
fn test_normalize_mach_override() {
    let mut mechanism = Mechanism {
        ty: Annotated::new("generic".to_string()),
        meta: Annotated::new(MechanismMeta {
            mach_exception: Annotated::new(MachException {
                ty: Annotated::new(1),
                code: Annotated::new(1),
                subcode: Annotated::new(8),
                name: Annotated::new("OVERRIDE".to_string()),
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    // We do not need SDK information here because mach exceptions only
    // occur on Darwin

    normalize_mechanism_meta(&mut mechanism, None);

    let mach_exception = mechanism.meta.0.unwrap().mach_exception.0.unwrap();
    assert_eq!(
        mach_exception,
        MachException {
            ty: Annotated::new(1),
            code: Annotated::new(1),
            subcode: Annotated::new(8),
            name: Annotated::new("OVERRIDE".to_string()),
        }
    );
}

#[test]
fn test_normalize_mach_fail() {
    let mut mechanism = Mechanism {
        ty: Annotated::new("generic".to_string()),
        meta: Annotated::new(MechanismMeta {
            mach_exception: Annotated::new(MachException {
                ty: Annotated::new(99),
                code: Annotated::new(1),
                subcode: Annotated::new(8),
                ..Default::default()
            }),
            ..Default::default()
        }),
        ..Default::default()
    };

    // We do not need SDK information here because mach exceptions only
    // occur on Darwin

    normalize_mechanism_meta(&mut mechanism, None);

    let mach_exception = mechanism.meta.0.unwrap().mach_exception.0.unwrap();
    assert_eq!(
        mach_exception,
        MachException {
            ty: Annotated::new(99),
            code: Annotated::new(1),
            subcode: Annotated::new(8),
            ..Default::default()
        }
    );
}
