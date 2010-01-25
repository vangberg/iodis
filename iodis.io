Iodis := Object clone do(
  host  := "localhost"
  port  := 6379
  debug := false

  connect := method(
    self socket := Socket clone setHost(host) setPort(port) connect
    self
  )

  callCommand := method(
    args    := call evalArgs flatten
    command := args removeFirst

    if (inlineCommands contains(command)) then(
      data := list(command, args) flatten remove(nil) join(" ") .. "\r\n"
    ) elseif(bulkCommands contains(command)) then(
      stream := args pop
      args = list(args, stream size) flatten join(" ")

      data := "#{command} #{args}\r\n#{stream}\r\n" interpolate
    ) elseif(multiBulkCommands contains(command)) then(
      args prepend(command)
      bulk := args map(arg, "$#{arg size}\r\n#{arg}\r\n" interpolate) join

      data := "*#{args size}\r\n#{bulk}" interpolate
    )

    if(debug, ("S: #{data}\n" interpolate print))

    socket streamWrite(data)

    replyProcessor perform(command) call(readReply)
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

  list(inlineCommands, bulkCommands, multiBulkCommands) flatten foreach(command,
    newSlot(command, doString(
      "method(callCommand(\"" .. command .. "\", call evalArgs))"
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

    forward := method(block(r, r))
  )
)
