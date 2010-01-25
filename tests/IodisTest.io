IodisTest := UnitTest clone do(
  setUp := method(
    self redis := Iodis clone connect
    redis flushall
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

  testRenamenx := method(
    redis set("foo", "value")
    redis set("bar", "other value")

    assertFalse(redis renamenx("foo", "bar"))
    assertTrue(redis renamenx("foo", "newKey"))
  )

  testDbsize := method(
    redis set("foo", "bar")
    redis set("lol", "cat")

    assertEquals(2, redis dbsize)
  )

  testExpire := method(
    redis set("foo", "bar")

    assertTrue(redis expire("foo", 123))
    assertFalse(redis expire("foo", 123))
  )

  testExpireat := method(
    redis set("foo", "bar")

    assertTrue(redis expireat("foo", 2123456789))
    assertFalse(redis expireat("foo", 2123456789))
  )

  testTtl := method(
    assertNil(redis ttl("foo"))

    redis set("foo", "bar")
    redis expire("foo", 1000)
    assertTrue(redis ttl("foo") > 0)
  )

  testMove := method(
    redis select(0)
    assertFalse(redis move("foo", 1))

    redis set("foo", "bar")
    assertTrue(redis move("foo", 1))
  )

  testSet := method(
    redis set("foo", "bar")
    assertEquals("bar", redis get("foo"))
  )

  testGet := method(
    assertNil(redis get("nonexisting"))

    redis set("foo", "bar")
    assertEquals("bar", redis get("foo"))
  )

  testGetSet := method(
    redis set("foo", "bar")

    assertEquals("bar", redis getset("foo", "new value"))
    assertEquals("new value", redis get("foo"))
  )

  testMget := method(
    redis set("foo", "bar")
    redis set("other", "value")

    r := redis mget("foo", "other", "third")
    assertTrue(r containsAll(list("bar", "value", nil)))
  )

  testSetnx := method(
    assertTrue(redis setnx("foo", "first value"))

    redis set("otherkey", "value")
    assertFalse(redis setnx("otherkey", "new value"))
  )

  testMset := method(
    redis mset("key1", "new value", "key2", "some value")

    assertEquals("new value", redis get("key1"))
    assertEquals("some value", redis get("key2"))
  )

  testMsetnx := method(
    assertTrue(redis msetnx("foo", "value", "bar", "other value"))

    assertFalse(redis msetnx("foo", "new value", "thirdkey", "blabla"))
  )
)
