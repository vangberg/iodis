IodisTest := UnitTest clone do(
  setUp := method(
    self redis := Iodis clone connect
    redis flushdb
  )
  
  testExists := method(
    redis set("foo", "bar")
    
    assertTrue(redis exists("foo"))
    assertFalse(redis exists("non-existing"))
  )

  testDel := method(
    assertEquals(0, redis del("non-existing"))

    redis set("foo", "bar")
    assertEquals(1, redis del("foo"))

    redis set("some", "value")
    redis set("other", "value")
    assertEquals(2, redis del("some", "other"))
  )

  testKeys := method(
    redis set("foo1", "yay")
    redis set("foo2", "woah")
    redis set("bar", "nay")

    keys := redis keys("foo*")
    assertEquals(2, keys size)
    assertTrue(keys containsAll(list("foo1", "foo2")))
  )

  testRename := method(
    redis set("some", "value")
    redis rename("some", "other")

    assertEquals("value", redis get("other"))
    assertFalse(redis exists("some"))
  )

  testRenameNx := method(
    redis set("foo", "value")
    redis set("bar", "other value")

    assertFalse(redis renameNx("foo", "bar"))
    assertTrue(redis renameNx("foo", "newKey"))
  )

  testDbSize := method(
    redis set("foo", "bar")
    redis set("lol", "cat")

    assertEquals(2, redis dbSize)
  )

  testExpire := method(
    redis set("foo", "bar")

    assertTrue(redis expire("foo", 123))
    assertFalse(redis expire("foo", 123))
  )

  testExpireAt := method(
    redis set("foo", "bar")

    assertTrue(redis expireAt("foo", 2123456789))
    assertFalse(redis expireAt("foo", 2123456789))
  )

  testTtl := method(
    assertNil(redis ttl("foo"))

    redis set("foo", "bar")
    redis expire("foo", 1000)
    assertTrue(redis ttl("foo") > 0)
  )
)
