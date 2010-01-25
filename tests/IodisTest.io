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

  testDoNotOverrideType := method(
    assertEquals("Iodis", redis type)
  )

  testTypeOf := method(
    redis set("foo", "bar")
    assertEquals("string", redis typeOf("foo"))
    redis del("foo")
    assertEquals("none", redis typeOf("foo"))
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

  testIncr := method(
    redis set("counter", 1)
    assertEquals(2, redis incr("counter"))
  )

  testIncrBy := method(
    redis set("counter", 1)
    assertEquals(5, redis incrby("counter", 4))
  )

  testDecr := method(
    redis set("counter", 5)
    assertEquals(4, redis decr("counter"))
  )

  testDecrBy := method(
    redis set("counter", 5)
    assertEquals(1, redis decrby("counter", 4))
  )

  testRpush := method(
    redis rpush("jobs", "some_job")
    assertEquals("list", redis typeOf("jobs"))
    assertEquals(1, redis llen("jobs"))
  )

  testLpush := method(
    redis lpush("jobs", "some_job")
    assertEquals("list", redis typeOf("jobs"))
    assertEquals(1, redis llen("jobs"))
  )

  testLLen := method(
    redis lpush("jobs", "some_job")
    redis lpush("jobs", "other")
    assertEquals(2, redis llen("jobs"))
  )

  testLrange := method(
    redis rpush("jobs", "job1")
    redis rpush("jobs", "job2")
    redis rpush("jobs", "job3")
    redis rpush("jobs", "job4")
    assertEquals(
      list("job2", "job3", "job4"),
      redis lrange("jobs", 1, -1)
    )
  )

  testLtrim := method(
    redis rpush("jobs", "job1")
    redis rpush("jobs", "job2")
    redis rpush("jobs", "job3")
    redis rpush("jobs", "job4")
    redis ltrim("jobs", 1, 2)
    assertEquals(
      list("job2", "job3"),
      redis lrange("jobs", 0, -1)
    )
  )

  testLindex := method(
    redis rpush("jobs", "job1")
    redis rpush("jobs", "job2")
    redis rpush("jobs", "job3")
    assertEquals("job2", redis lindex("jobs", 1))
  )

  testLset := method(
    redis rpush("jobs", "job1")
    redis rpush("jobs", "job2")
    redis rpush("jobs", "job3")
    redis lset("jobs", 1, "new job")
    assertEquals("new job", redis lindex("jobs", 1))
  )

  testLrem := method(
    redis rpush("jobs", "job1")
    redis rpush("jobs", "job2")
    redis rpush("jobs", "job1")

    assertEquals(2, redis lrem("jobs", 2, "job1"))
    assertEquals(1, redis llen("jobs"))
  )

  testLpop := method(
    redis rpush("jobs", "job1")
    redis rpush("jobs", "job2")
    assertEquals("job1", redis lpop("jobs"))
  )

  testRpop := method(
    redis rpush("jobs", "job1")
    redis rpush("jobs", "job2")
    assertEquals("job2", redis rpop("jobs"))
  )

  testBlpop := method(
    redis rpush("jobs", "job1")
    assertEquals(
      list("jobs", "job1"),
      redis blpop("jobs", "queue1")
    )
  )

  testBrpop := method(
    redis rpush("jobs", "job1")
    assertEquals(
      list("jobs", "job1"),
      redis brpop("jobs", "queue1")
    )
  )

  testRpoplpush := method(
    redis lpush("new-jobs", "job1")
    redis lpush("new-jobs", "job2")
    assertEquals(
      "job1",
      redis rpoplpush("new-jobs", "ongoing-jobs")
    )
    assertEquals("job1", redis rpop("ongoing-jobs"))
  )
)
