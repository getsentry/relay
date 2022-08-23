use crate::protocol::{Context, ContextInner, Event, Mechanism};
use crate::types::{Annotated, Error, ProcessingAction, ProcessingResult};

#[cfg(test)]
use crate::protocol::{CError, MachException, MechanismMeta, PosixSignal};

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

/// Internal utility trait to indicate the OS.
#[derive(Debug, PartialEq, Clone, Copy)]
pub enum OsHint {
    Windows,
    Linux,
    Darwin,
}

impl OsHint {
    fn from_name(name: &str) -> Option<OsHint> {
        match name.to_lowercase().as_ref() {
            "ios" | "watchos" | "tvos" | "macos" => Some(OsHint::Darwin),
            "linux" | "android" => Some(OsHint::Linux),
            "windows" => Some(OsHint::Windows),
            _ => None,
        }
    }

    pub fn from_event(event: &Event) -> Option<OsHint> {
        if let Some(debug_meta) = event.debug_meta.value() {
            if let Some(sdk_info) = debug_meta.system_sdk.value() {
                if let Some(name) = sdk_info.sdk_name.as_str() {
                    return Self::from_name(name);
                }
            }
        }

        if let Some(contexts) = event.contexts.value() {
            if let Some(context) = contexts.get("os") {
                if let Some(&ContextInner(Context::Os(ref os_context))) = context.value() {
                    if let Some(name) = os_context.name.as_str() {
                        return Self::from_name(name);
                    }
                }
            }
        }

        None
    }
}

/// Normalizes the exception mechanism in place.
pub fn normalize_mechanism(mechanism: &mut Mechanism, os_hint: Option<OsHint>) -> ProcessingResult {
    mechanism.help_link.apply(|value, meta| {
        if value.starts_with("http://") || value.starts_with("https://") {
            Ok(())
        } else {
            meta.add_error(Error::expected("http URL"));
            Err(ProcessingAction::DeleteValueSoft)
        }
    })?;

    let meta = match mechanism.meta.value_mut() {
        Some(meta) => meta,
        None => return Ok(()),
    };

    if let Some(os_hint) = os_hint {
        if let Some(cerror) = meta.errno.value_mut() {
            if cerror.name.value().is_none() {
                if let Some(errno) = cerror.number.value() {
                    if let Some(name) = get_errno_name(*errno, os_hint) {
                        cerror.name = Annotated::new(name.to_string());
                    }
                }
            }
        }

        if let Some(signal) = meta.signal.value_mut() {
            if let Some(signo) = signal.number.value() {
                if signal.name.value().is_none() {
                    if let Some(name) = get_signal_name(*signo, os_hint) {
                        signal.name = Annotated::new(name.to_owned());
                    }
                }

                if os_hint == OsHint::Darwin && signal.code_name.value().is_none() {
                    if let Some(code) = signal.code.value() {
                        if let Some(code_name) = get_signal_code_name(*signo, *code) {
                            signal.code_name = Annotated::new(code_name.to_string());
                        }
                    }
                }
            }
        }
    }

    if let Some(mach_exception) = meta.mach_exception.value_mut() {
        if let Some(number) = mach_exception.ty.value() {
            if mach_exception.name.value().is_none() {
                if let Some(name) = get_mach_exception_name(*number) {
                    mach_exception.name = Annotated::new(name.to_owned());
                }
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use similar_asserts::assert_eq;

    use super::*;

    #[test]
    fn test_normalize_missing() {
        let mut mechanism = Mechanism {
            ty: Annotated::new("generic".to_string()),
            ..Default::default()
        };

        let old_mechanism = mechanism.clone();

        normalize_mechanism(&mut mechanism, None).unwrap();

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

        normalize_mechanism(&mut mechanism, Some(OsHint::Linux)).unwrap();

        let errno = mechanism.meta.value().unwrap().errno.value().unwrap();
        assert_eq!(
            errno,
            &CError {
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

        normalize_mechanism(&mut mechanism, Some(OsHint::Linux)).unwrap();

        let errno = mechanism.meta.value().unwrap().errno.value().unwrap();
        assert_eq!(
            errno,
            &CError {
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

        normalize_mechanism(&mut mechanism, None).unwrap();

        let errno = mechanism.meta.value().unwrap().errno.value().unwrap();
        assert_eq!(
            errno,
            &CError {
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

        normalize_mechanism(&mut mechanism, Some(OsHint::Darwin)).unwrap();

        let signal = mechanism.meta.value().unwrap().signal.value().unwrap();
        assert_eq!(
            signal,
            &PosixSignal {
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

        normalize_mechanism(&mut mechanism, Some(OsHint::Linux)).unwrap();

        let signal = mechanism.meta.value().unwrap().signal.value().unwrap();

        assert_eq!(
            signal,
            &PosixSignal {
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

        normalize_mechanism(&mut mechanism, Some(OsHint::Linux)).unwrap();

        let signal = mechanism.meta.value().unwrap().signal.value().unwrap();

        assert_eq!(
            signal,
            &PosixSignal {
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

        normalize_mechanism(&mut mechanism, None).unwrap();

        let signal = mechanism.meta.value().unwrap().signal.value().unwrap();

        assert_eq!(
            signal,
            &PosixSignal {
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

        normalize_mechanism(&mut mechanism, None).unwrap();

        let mach_exception = mechanism
            .meta
            .value()
            .unwrap()
            .mach_exception
            .value()
            .unwrap();

        assert_eq!(
            mach_exception,
            &MachException {
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

        normalize_mechanism(&mut mechanism, None).unwrap();

        let mach_exception = mechanism
            .meta
            .value()
            .unwrap()
            .mach_exception
            .value()
            .unwrap();
        assert_eq!(
            mach_exception,
            &MachException {
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

        normalize_mechanism(&mut mechanism, None).unwrap();

        let mach_exception = mechanism
            .meta
            .value()
            .unwrap()
            .mach_exception
            .value()
            .unwrap();
        assert_eq!(
            mach_exception,
            &MachException {
                ty: Annotated::new(99),
                code: Annotated::new(1),
                subcode: Annotated::new(8),
                ..Default::default()
            }
        );
    }

    #[test]
    fn test_normalize_http_url() {
        use crate::types::SerializableAnnotated;
        use insta::assert_ron_snapshot;

        let mut good_mechanism = Mechanism {
            ty: Annotated::new("generic".to_string()),
            help_link: Annotated::new("https://example.com/".to_string()),
            ..Default::default()
        };

        normalize_mechanism(&mut good_mechanism, None).unwrap();
        assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(good_mechanism)), @r###"
    {
      "type": "generic",
      "help_link": "https://example.com/",
    }
    "###);

        let mut bad_mechanism = Mechanism {
            ty: Annotated::new("generic".to_string()),
            help_link: Annotated::new("javascript:alert(document.cookie)".to_string()),
            ..Default::default()
        };

        normalize_mechanism(&mut bad_mechanism, None).unwrap();
        assert_ron_snapshot!(SerializableAnnotated(&Annotated::new(bad_mechanism)), @r###"
    {
      "type": "generic",
      "help_link": (),
      "_meta": {
        "help_link": {
          "": Meta(Some(MetaInner(
            err: [
              [
                "invalid_data",
                {
                  "reason": "expected http URL",
                },
              ],
            ],
            val: Some("javascript:alert(document.cookie)"),
          ))),
        },
      },
    }
    "###);
    }
}
