Iodis := Object clone do(
  host  := "localhost"
  port  := 6379
  debug := false

  connect := method(
    self socket := Socket clone setHost(host) setPort(port) connect
    self
  )

  writeData := method(data,
    if(debug, ("S: " .. data) print)
    socket streamWrite(data)
    readReply
  )

  readReply := method(
    responseType  := socket readBytes(1)
    line          := socket readUntilSeq("\r\n")

    responseType switch(
      "+",  line,
      ":",  line asNumber,
      "$",  if (line asNumber < 0, nil,
                data := socket readBytes(line asNumber)
                socket readBytes(2)
                data),
      "*",  if (line asNumber < 0, nil,
                values := list()
                line asNumber repeat(
                  values push(readReply)
                )
                values)
    ) 
  )

  inlineCommand := method(command, args,
    data := list(command, args) remove(nil) flatten join(" ") .. "\r\n"
    writeData(data)
  )

  bulkCommand := method(command, args, stream,
    data := list(command, args, stream size) flatten join(" ") .. "\r\n"
    data = data .. stream .. "\r\n"
    writeData(data)
  )

  flushdb := method(inlineCommand("FLUSHDB"))

  exists := method(key,
    r := inlineCommand("EXISTS", key)
    if(r == 1, true, false)
  )

  del := method(
    inlineCommand("DEL", call evalArgs)
  )

  keys := method(pattern,
    inlineCommand("KEYS", pattern) split(" ")
  )

  randomKey := method(
    inlineCommand("RANDOMKEY")
  )

  rename := method(old, new,
    inlineCommand("RENAME", list(old, new))
  )

  renameNx := method(old, new,
    r := inlineCommand("RENAMENX", list(old, new))
    if(r == 1, true, false)
  )

  dbSize := method(
    inlineCommand("DBSIZE")
  )
  
  expire := method(key, seconds,
    r := inlineCommand("EXPIRE", list(key, seconds))
    if(r == 1, true, false)
  )

  expireAt := method(key, unix,
    r := inlineCommand("EXPIREAT", list(key, unix))
    if(r == 1, true, false)
  )

  ttl := method(key,
    r := inlineCommand("TTL", key)
    if(r < 0, nil, r)
  )

  set := method(key, value, bulkCommand("SET", key, value)) 
  get := method(key, inlineCommand("GET", key))
  mget := method(keys, inlineCommand("MGET", keys))
  incr := method(key, inlineCommand("INCR", key))
  decr := method(key, inlineCommand("DECR", key))

  lpush := method(key, value, bulkCommand("RPUSH", key, value))
  lrange := method(key, start, last,
    inlineCommand("LRANGE", list(key, start, last))
  )
  rpop := method(key, inlineCommand("RPOP", key))

  ping := method(inlineCommand("PING"))
)
