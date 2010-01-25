Iodis := Object clone do(
  host  := "localhost"
  port  := 6379
  debug := false

  connect := method(
    self socket := Socket clone setHost(host) setPort(port) connect
    self
  )

  callCommand := method(command, args, stream,
    data := list(command, args) flatten remove(nil) join(" ") .. "\r\n"
    if(stream, data = data .. stream)

    if(debug, (list("S:", data, "\n") join(" ") print))

    socket streamWrite(data)

    reply := readReply
    if (replyProcessor hasSlot(command),
        replyProcessor getSlot(command) call(reply),
        reply)
  )

  inlineCommands := list(
    "exists", "del", "keys", "randomkey", "rename", "renamenx", "dbsize", 
    "expire", "expireat", "ttl", "select", "move", "flushdb", "flushall",

    "get", "mget"
  )

  bulkCommands := list(
    "set", "getset", "setnx"
  )

  multiBulkCommands := list(
    "mset", "msetnx"
  )

  inlineCommands foreach(command,
    newSlot(command, doString(
      "method(callCommand(\"" .. command .. "\", call evalArgs))"
    ))
  )

  bulkCommand := method(
    args    := call evalArgs flatten
    command := args removeFirst
    stream  := args pop

    args = list(args, stream size) flatten join(" ")

    callCommand(command, args, stream .. "\r\n")
  )

  bulkCommands foreach(command,
    newSlot(command, doString(
      "method(bulkCommand(\"" .. command .. "\", call evalArgs))"
    ))
  )

  multiBulkCommand := method(
    args    := call evalArgs flatten
    command := "*" .. args size
    data    := args map(arg, "$#{arg size}\r\n#{arg}\r\n" interpolate) join

    callCommand(command, nil, data)
  )

  multiBulkCommands foreach(command,
    newSlot(command, doString(
      "method(multiBulkCommand(\"" .. command .. "\", call evalArgs))"
    ))
  )

  readReply := method(
    responseType  := socket readBytes(1)
    line          := socket readUntilSeq("\r\n")

    responseType switch(
      "+",  line,
      ":",  line asNumber,
      "$",  line asNumber compare(0) switch(
                -1, nil,
                 0, socket readBytes(2)
                    "",
                 1, data := socket readBytes(line asNumber)
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

  replyProcessor := Object clone do(
    Boolean   := block(r, r == 1)

    flushdb   := Boolean
    exists    := Boolean
    keys      := block(r, r split(" "))
    renamenx  := Boolean
    expire    := Boolean
    expireat  := Boolean
    ttl       := block(r, if(r < 0, nil, r))
    move      := Boolean
    setnx     := Boolean
    msetnx    := Boolean
  )

)
