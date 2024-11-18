use std::sync::LazyLock;

use itertools::Itertools;
use regex::Regex;

/// Returns the substring that matches a known redis command.
pub fn matching_redis_command(seek: &str) -> Option<&str> {
    let m = COMMAND_REGEX.captures(seek)?.get(1)?;
    Some(m.as_str())
}

/// List of redis commands.
///
/// Make sure this list remains sorted, such that long prefixes match before short prefixes.
///
/// See <https://redis.io/docs/latest/commands/>.
const REDIS_COMMANDS: &[&str] = &[
    "ACL",
    "ACL CAT",
    "ACL DELUSER",
    "ACL DRYRUN",
    "ACL GENPASS",
    "ACL GETUSER",
    "ACL HELP",
    "ACL LIST",
    "ACL LOAD",
    "ACL LOG",
    "ACL SAVE",
    "ACL SETUSER",
    "ACL USERS",
    "ACL WHOAMI",
    "APPEND",
    "ASKING",
    "AUTH",
    "BF.ADD",
    "BF.CARD",
    "BF.EXISTS",
    "BF.INFO",
    "BF.INSERT",
    "BF.LOADCHUNK",
    "BF.MADD",
    "BF.MEXISTS",
    "BF.RESERVE",
    "BF.SCANDUMP",
    "BGREWRITEAOF",
    "BGSAVE",
    "BITCOUNT",
    "BITFIELD",
    "BITFIELD_RO",
    "BITOP",
    "BITPOS",
    "BLMOVE",
    "BLMPOP",
    "BLPOP",
    "BRPOP",
    "BRPOPLPUSH",
    "BZMPOP",
    "BZPOPMAX",
    "BZPOPMIN",
    "CF.ADD",
    "CF.ADDNX",
    "CF.COUNT",
    "CF.DEL",
    "CF.EXISTS",
    "CF.INFO",
    "CF.INSERT",
    "CF.INSERTNX",
    "CF.LOADCHUNK",
    "CF.MEXISTS",
    "CF.RESERVE",
    "CF.SCANDUMP",
    "CLIENT",
    "CLIENT CACHING",
    "CLIENT GETNAME",
    "CLIENT GETREDIR",
    "CLIENT HELP",
    "CLIENT ID",
    "CLIENT INFO",
    "CLIENT KILL",
    "CLIENT LIST",
    "CLIENT NO-EVICT",
    "CLIENT NO-TOUCH",
    "CLIENT PAUSE",
    "CLIENT REPLY",
    "CLIENT SETINFO",
    "CLIENT SETNAME",
    "CLIENT TRACKING",
    "CLIENT TRACKINGINFO",
    "CLIENT UNBLOCK",
    "CLIENT UNPAUSE",
    "CLUSTER",
    "CLUSTER ADDSLOTS",
    "CLUSTER ADDSLOTSRANGE",
    "CLUSTER BUMPEPOCH",
    "CLUSTER COUNT-FAILURE-REPORTS",
    "CLUSTER COUNTKEYSINSLOT",
    "CLUSTER DELSLOTS",
    "CLUSTER DELSLOTSRANGE",
    "CLUSTER FAILOVER",
    "CLUSTER FLUSHSLOTS",
    "CLUSTER FORGET",
    "CLUSTER GETKEYSINSLOT",
    "CLUSTER HELP",
    "CLUSTER INFO",
    "CLUSTER KEYSLOT",
    "CLUSTER LINKS",
    "CLUSTER MEET",
    "CLUSTER MYID",
    "CLUSTER MYSHARDID",
    "CLUSTER NODES",
    "CLUSTER REPLICAS",
    "CLUSTER REPLICATE",
    "CLUSTER RESET",
    "CLUSTER SAVECONFIG",
    "CLUSTER SET-CONFIG-EPOCH",
    "CLUSTER SETSLOT",
    "CLUSTER SHARDS",
    "CLUSTER SLAVES",
    "CLUSTER SLOTS",
    "CMS.INCRBY",
    "CMS.INFO",
    "CMS.INITBYDIM",
    "CMS.INITBYPROB",
    "CMS.MERGE",
    "CMS.QUERY",
    "COMMAND",
    "COMMAND COUNT",
    "COMMAND DOCS",
    "COMMAND GETKEYS",
    "COMMAND GETKEYSANDFLAGS",
    "COMMAND HELP",
    "COMMAND INFO",
    "COMMAND LIST",
    "CONFIG",
    "CONFIG GET",
    "CONFIG HELP",
    "CONFIG RESETSTAT",
    "CONFIG REWRITE",
    "CONFIG SET",
    "COPY",
    "DBSIZE",
    "DEBUG",
    "DECR",
    "DECRBY",
    "DEL",
    "DISCARD",
    "DUMP",
    "ECHO",
    "EVAL",
    "EVAL_RO",
    "EVALSHA",
    "EVALSHA_RO",
    "EXEC",
    "EXISTS",
    "EXPIRE",
    "EXPIREAT",
    "EXPIRETIME",
    "FAILOVER",
    "FCALL",
    "FCALL_RO",
    "FLUSHALL",
    "FLUSHDB",
    "FT._LIST",
    "FT.AGGREGATE",
    "FT.ALIASADD",
    "FT.ALIASDEL",
    "FT.ALIASUPDATE",
    "FT.ALTER",
    "FT.CONFIG GET",
    "FT.CONFIG HELP",
    "FT.CONFIG SET",
    "FT.CREATE",
    "FT.CURSOR DEL",
    "FT.CURSOR READ",
    "FT.DICTADD",
    "FT.DICTDEL",
    "FT.DICTDUMP",
    "FT.DROPINDEX",
    "FT.EXPLAIN",
    "FT.EXPLAINCLI",
    "FT.INFO",
    "FT.PROFILE",
    "FT.SEARCH",
    "FT.SPELLCHECK",
    "FT.SUGADD",
    "FT.SUGDEL",
    "FT.SUGGET",
    "FT.SUGLEN",
    "FT.SYNDUMP",
    "FT.SYNUPDATE",
    "FT.TAGVALS",
    "FUNCTION",
    "FUNCTION DELETE",
    "FUNCTION DUMP",
    "FUNCTION FLUSH",
    "FUNCTION HELP",
    "FUNCTION KILL",
    "FUNCTION LIST",
    "FUNCTION LOAD",
    "FUNCTION RESTORE",
    "FUNCTION STATS",
    "GEOADD",
    "GEODIST",
    "GEOHASH",
    "GEOPOS",
    "GEORADIUS",
    "GEORADIUS_RO",
    "GEORADIUSBYMEMBER",
    "GEORADIUSBYMEMBER_RO",
    "GEOSEARCH",
    "GEOSEARCHSTORE",
    "GET",
    "GETBIT",
    "GETDEL",
    "GETEX",
    "GETRANGE",
    "GETSET",
    "HDEL",
    "HELLO",
    "HEXISTS",
    "HEXPIRE",
    "HEXPIREAT",
    "HEXPIRETIME",
    "HGET",
    "HGETALL",
    "HINCRBY",
    "HINCRBYFLOAT",
    "HKEYS",
    "HLEN",
    "HMGET",
    "HMSET",
    "HPERSIST",
    "HPEXPIRE",
    "HPEXPIREAT",
    "HPEXPIRETIME",
    "HPTTL",
    "HRANDFIELD",
    "HSCAN",
    "HSET",
    "HSETNX",
    "HSTRLEN",
    "HTTL",
    "HVALS",
    "INCR",
    "INCRBY",
    "INCRBYFLOAT",
    "INFO",
    "JSON.ARRAPPEND",
    "JSON.ARRINDEX",
    "JSON.ARRINSERT",
    "JSON.ARRLEN",
    "JSON.ARRPOP",
    "JSON.ARRTRIM",
    "JSON.CLEAR",
    "JSON.DEBUG",
    "JSON.DEBUG HELP",
    "JSON.DEBUG MEMORY",
    "JSON.DEL",
    "JSON.FORGET",
    "JSON.GET",
    "JSON.MERGE",
    "JSON.MGET",
    "JSON.MSET",
    "JSON.NUMINCRBY",
    "JSON.NUMMULTBY",
    "JSON.OBJKEYS",
    "JSON.OBJLEN",
    "JSON.RESP",
    "JSON.SET",
    "JSON.STRAPPEND",
    "JSON.STRLEN",
    "JSON.TOGGLE",
    "JSON.TYPE",
    "KEYS",
    "LASTSAVE",
    "LATENCY",
    "LATENCY DOCTOR",
    "LATENCY GRAPH",
    "LATENCY HELP",
    "LATENCY HISTOGRAM",
    "LATENCY HISTORY",
    "LATENCY LATEST",
    "LATENCY RESET",
    "LCS",
    "LINDEX",
    "LINSERT",
    "LLEN",
    "LMOVE",
    "LMPOP",
    "LOLWUT",
    "LPOP",
    "LPOS",
    "LPUSH",
    "LPUSHX",
    "LRANGE",
    "LREM",
    "LSET",
    "LTRIM",
    "MEMORY",
    "MEMORY DOCTOR",
    "MEMORY HELP",
    "MEMORY MALLOC-STATS",
    "MEMORY PURGE",
    "MEMORY STATS",
    "MEMORY USAGE",
    "MGET",
    "MIGRATE",
    "MODULE",
    "MODULE HELP",
    "MODULE LIST",
    "MODULE LOAD",
    "MODULE LOADEX",
    "MODULE UNLOAD",
    "MONITOR",
    "MOVE",
    "MSET",
    "MSETNX",
    "MULTI",
    "OBJECT",
    "OBJECT ENCODING",
    "OBJECT FREQ",
    "OBJECT HELP",
    "OBJECT IDLETIME",
    "OBJECT REFCOUNT",
    "PERSIST",
    "PEXPIRE",
    "PEXPIREAT",
    "PEXPIRETIME",
    "PFADD",
    "PFCOUNT",
    "PFDEBUG",
    "PFMERGE",
    "PFSELFTEST",
    "PING",
    "PSETEX",
    "PSUBSCRIBE",
    "PSYNC",
    "PTTL",
    "PUBLISH",
    "PUBSUB",
    "PUBSUB CHANNELS",
    "PUBSUB HELP",
    "PUBSUB NUMPAT",
    "PUBSUB NUMSUB",
    "PUBSUB SHARDCHANNELS",
    "PUBSUB SHARDNUMSUB",
    "PUNSUBSCRIBE",
    "QUIT",
    "RANDOMKEY",
    "READONLY",
    "READWRITE",
    "RENAME",
    "RENAMENX",
    "REPLCONF",
    "REPLICAOF",
    "RESET",
    "RESTORE",
    "RESTORE-ASKING",
    "ROLE",
    "RPOP",
    "RPOPLPUSH",
    "RPUSH",
    "RPUSHX",
    "SADD",
    "SAVE",
    "SCAN",
    "SCARD",
    "SCRIPT",
    "SCRIPT DEBUG",
    "SCRIPT EXISTS",
    "SCRIPT FLUSH",
    "SCRIPT HELP",
    "SCRIPT KILL",
    "SCRIPT LOAD",
    "SDIFF",
    "SDIFFSTORE",
    "SELECT",
    "SET",
    "SETBIT",
    "SETEX",
    "SETNX",
    "SETRANGE",
    "SHUTDOWN",
    "SINTER",
    "SINTERCARD",
    "SINTERSTORE",
    "SISMEMBER",
    "SLAVEOF",
    "SLOWLOG",
    "SLOWLOG GET",
    "SLOWLOG HELP",
    "SLOWLOG LEN",
    "SLOWLOG RESET",
    "SMEMBERS",
    "SMISMEMBER",
    "SMOVE",
    "SORT",
    "SORT_RO",
    "SPOP",
    "SPUBLISH",
    "SRANDMEMBER",
    "SREM",
    "SSCAN",
    "SSUBSCRIBE",
    "STRLEN",
    "SUBSCRIBE",
    "SUBSTR",
    "SUNION",
    "SUNIONSTORE",
    "SUNSUBSCRIBE",
    "SWAPDB",
    "SYNC",
    "TDIGEST.ADD",
    "TDIGEST.BYRANK",
    "TDIGEST.BYREVRANK",
    "TDIGEST.CDF",
    "TDIGEST.CREATE",
    "TDIGEST.INFO",
    "TDIGEST.MAX",
    "TDIGEST.MERGE",
    "TDIGEST.MIN",
    "TDIGEST.QUANTILE",
    "TDIGEST.RANK",
    "TDIGEST.RESET",
    "TDIGEST.REVRANK",
    "TDIGEST.TRIMMED_MEAN",
    "TIME",
    "TOPK.ADD",
    "TOPK.COUNT",
    "TOPK.INCRBY",
    "TOPK.INFO",
    "TOPK.LIST",
    "TOPK.QUERY",
    "TOPK.RESERVE",
    "TOUCH",
    "TS.ADD",
    "TS.ALTER",
    "TS.CREATE",
    "TS.CREATERULE",
    "TS.DECRBY",
    "TS.DEL",
    "TS.DELETERULE",
    "TS.GET",
    "TS.INCRBY",
    "TS.INFO",
    "TS.MADD",
    "TS.MGET",
    "TS.MRANGE",
    "TS.MREVRANGE",
    "TS.QUERYINDEX",
    "TS.RANGE",
    "TS.REVRANGE",
    "TTL",
    "TYPE",
    "UNLINK",
    "UNSUBSCRIBE",
    "UNWATCH",
    "WAIT",
    "WAITAOF",
    "WATCH",
    "XACK",
    "XADD",
    "XAUTOCLAIM",
    "XCLAIM",
    "XDEL",
    "XGROUP",
    "XGROUP CREATE",
    "XGROUP CREATECONSUMER",
    "XGROUP DELCONSUMER",
    "XGROUP DESTROY",
    "XGROUP HELP",
    "XGROUP SETID",
    "XINFO",
    "XINFO CONSUMERS",
    "XINFO GROUPS",
    "XINFO HELP",
    "XINFO STREAM",
    "XLEN",
    "XPENDING",
    "XRANGE",
    "XREAD",
    "XREADGROUP",
    "XREVRANGE",
    "XSETID",
    "XTRIM",
    "ZADD",
    "ZCARD",
    "ZCOUNT",
    "ZDIFF",
    "ZDIFFSTORE",
    "ZINCRBY",
    "ZINTER",
    "ZINTERCARD",
    "ZINTERSTORE",
    "ZLEXCOUNT",
    "ZMPOP",
    "ZMSCORE",
    "ZPOPMAX",
    "ZPOPMIN",
    "ZRANDMEMBER",
    "ZRANGE",
    "ZRANGEBYLEX",
    "ZRANGEBYSCORE",
    "ZRANGESTORE",
    "ZRANK",
    "ZREM",
    "ZREMRANGEBYLEX",
    "ZREMRANGEBYRANK",
    "ZREMRANGEBYSCORE",
    "ZREVRANGE",
    "ZREVRANGEBYLEX",
    "ZREVRANGEBYSCORE",
    "ZREVRANK",
    "ZSCAN",
    "ZSCORE",
    "ZUNION",
    "ZUNIONSTORE",
];

static COMMAND_REGEX: LazyLock<Regex> = LazyLock::new(|| {
    // Combine in reverse order, such that longer matches are matched first.
    let commands = REDIS_COMMANDS
        .iter()
        .rev()
        .map(|s| s.replace('.', r"\."))
        .join("|");
    let regex = format!("(?P<command>({commands}))(\\s|$)");
    regex::RegexBuilder::new(&regex)
        .case_insensitive(true)
        .unicode(false)
        .build()
        .unwrap()
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_exact() {
        assert_eq!(matching_redis_command("TOPK.ADD"), Some("TOPK.ADD"));
    }

    #[test]
    fn same_length_arbitrary_case() {
        assert_eq!(matching_redis_command("TopK.add"), Some("TopK.add"));
    }

    #[test]
    fn same_length_different_content() {
        assert_eq!(matching_redis_command("_OPK.ADD"), None);
    }

    #[test]
    fn same_length_different_content2() {
        assert_eq!(matching_redis_command("T_PK.ADD"), None);
    }

    #[test]
    fn same_length_different_content3() {
        assert_eq!(matching_redis_command("TOPK.AD_"), None);
    }

    #[test]
    fn prefix() {
        assert_eq!(
            matching_redis_command("TOPK.ADD something something"),
            Some("TOPK.ADD")
        );
    }

    #[test]
    fn prefix_arbitrary_case() {
        assert_eq!(
            matching_redis_command("TopK.add Something SomeThing"),
            Some("TopK.add")
        );
    }

    #[test]
    fn needle_too_short() {
        assert_eq!(matching_redis_command("TOPK.AD"), None);
    }

    #[test]
    fn needle_too_short_arbitrary_case() {
        assert_eq!(matching_redis_command("TopK.ad"), None);
    }

    #[test]
    fn empty_needle() {
        assert_eq!(matching_redis_command(""), None);
    }

    #[test]
    fn find_word_boundary() {
        assert_eq!(matching_redis_command("ACL cattington"), Some("ACL"));
    }

    #[test]
    fn find_with_extra_word() {
        assert_eq!(matching_redis_command("ACL cat tington"), Some("ACL cat"));
    }

    #[test]
    fn find_ambiguous_prefix() {
        assert_eq!(matching_redis_command("ACL CAT"), Some("ACL CAT"));
    }
}
