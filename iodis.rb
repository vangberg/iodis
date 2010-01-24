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
    responseType  := socket readBytes(1)
    line          := socket readUntilSeq("\r\n")

    response := responseType switch(
      "+", line,
      ":", line asNumber,
      "$", socket readBytes(line asNumber)
    ) clone

    socket readBuffer empty

    response
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
  incr := method(key, inlineCommand("INCR", key))
  decr := method(key, inlineCommand("DECR", key))

  ping := method(inlineCommand("PING"))
)

i := Iodis clone connect
i ping print
i get("foo") print
i incr("catz0r") print
i incr("catz0r") print
i incr("catz0r") print
