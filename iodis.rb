Iodis := Object clone do(
  connect := method(
    self socket := Socket clone setHost("localhost") setPort(6379) connect
    self
  )

  writeData := method(data,
    socket streamWrite(data)
    socket streamReadNextChunk
    processResponse
  )

  processResponse := method(
    response := readReply
    socket readBuffer empty
    response
  )

  readReply := method(
    responseType  := socket readBytes(1)
    line          := socket readUntilSeq("\r\n")

    responseType switch(
      "+",  line,
      ":",  line asNumber,
      "$",  socket readBytes(line asNumber) clone,
      "*",  values := list()
            line asNumber repeat(
              values push(readReply)
              socket readBytes(2)
            )
            values
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

  set := method(key, value, bulkCommand("SET", key, value)) 
  get := method(key, inlineCommand("GET", key))
  mget := method(keys, inlineCommand("MGET", keys))
  incr := method(key, inlineCommand("INCR", key))
  decr := method(key, inlineCommand("DECR", key))

  ping := method(inlineCommand("PING"))
)

i := Iodis clone connect
i set("y0", "hey") print
"\n" print
i set("broke", "hephep") print
"\n" print
i get("y0") print
"\n" print
"\n" print
i mget(list("y0", "broke")) foreach(x,x print)
