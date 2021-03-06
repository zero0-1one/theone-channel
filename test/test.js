'use strict'

const assert = require('chai').assert
const Db = require('zo-theone').Db
const { its_par } = require('zo-mocha-ext')
const channel = require('../index')

const options = {
  'name': 'db',
  'host': 'localhost',
  'user': 'theone_tester',
  'password': '12345',
  'database': 'theone_test',
  'connectionLimit': 5
}

async function sleep(time) {
  return new Promise(resolve => {
    setTimeout(resolve, time)
  })
}


let data = []
let N = 4

let config = [
  { timeBefore: 0, timeAfter: 300, channel: ['a', 'b'] },
  { timeBefore: 100, timeAfter: 100, channel: ['b'] },
  { timeBefore: 200, timeAfter: 0, channel: ['c', 'd'] },
  { timeBefore: 400, timeAfter: 100, channel: ['d', 'a'] },
]

describe('db', function () {

  let channelInst = null
  its_par(N, 'lock crc32 相同 channel 实例', async function () {
    let cfg = config[this.iteration]
    await this.beforeAll(async () => {
      data = []
      channelInst = await channel.init(options, 'same', Db)
    })
    await Db.transaction(async (db) => {
      await sleep(cfg.timeBefore)
      await channelInst.lock(db, cfg.channel)

      data.push(this.iteration)
      await sleep(cfg.timeAfter)
    }, options)

    await this.afterAll(async () => {
      assert.deepEqual(data, [0, 2, 1, 3])
    })
  })



  its_par(N, 'lock crc32 不同 channel 实例', async function () {
    let cfg = config[this.iteration]
    await this.beforeAll(async () => {
      data = []
    })
    let inst = await channel.init(options, 'diff' + this.iteration, Db)

    await Db.transaction(async (db) => {
      await sleep(cfg.timeBefore)
      await inst.lock(db, cfg.channel)

      data.push(this.iteration)
      await sleep(cfg.timeAfter)
    }, options)
    await this.afterAll(async () => {
      assert.deepEqual(data, [0, 1, 2, 3])
    })
  })

  its_par(N, 'lock string 相同 channel 实例', async function () {
    let cfg = config[this.iteration]
    await this.beforeAll(async () => {
      data = []
      channelInst = await channel.init(options, 'same', Db)
    })
    await Db.transaction(async (db) => {
      await sleep(cfg.timeBefore)
      await channelInst.lock(db, cfg.channel, 'string')

      data.push(this.iteration)
      await sleep(cfg.timeAfter)
    }, options)

    await this.afterAll(async () => {
      assert.deepEqual(data, [0, 2, 1, 3])
    })
  })



  its_par(N, 'lock string 不同 channel 实例', async function () {
    let cfg = config[this.iteration]
    await this.beforeAll(async () => {
      data = []
    })
    let inst = await channel.init(options, 'diff' + this.iteration, Db)

    await Db.transaction(async (db) => {
      await sleep(cfg.timeBefore)
      await inst.lock(db, cfg.channel, 'string')

      data.push(this.iteration)
      await sleep(cfg.timeAfter)
    }, options)
    await this.afterAll(async () => {
      assert.deepEqual(data, [0, 1, 2, 3])
    })
  })


  it('lockNowait crc32 失败', async function () {
    let inst = await channel.init(options, 'test', Db)
    await Db.transaction(async (db1) => {
      await inst.lockNowait(db1, 'abc', 'crc32')

      await Db.transaction(async (db2) => {
        let rt = await inst.lockNowait(db2, 'abc', 'crc32')
        assert.isFalse(rt)
      }, options)

    }, options)
  })

  it('lockNowait crc32 成功', async function () {
    let inst = await channel.init(options, 'test', Db)
    await Db.transaction(async (db1) => {
      await inst.lockNowait(db1, 'abc', 'crc32')

      await Db.transaction(async (db2) => {
        let rt = await inst.lockNowait(db2, 'efg', 'crc32')
        assert.isTrue(rt)
      }, options)

    }, options)
  })


  it('lockNowait string 失败', async function () {
    let inst = await channel.init(options, 'test', Db)
    await Db.transaction(async (db1) => {
      await inst.lockNowait(db1, 'abc')

      await Db.transaction(async (db2) => {
        let rt = await inst.lockNowait(db2, 'abc')
        assert.isFalse(rt)
      }, options)

    }, options)
  })

  it('lockNowait string 成功', async function () {
    let inst = await channel.init(options, 'test', Db)
    await Db.transaction(async (db1) => {
      await inst.lockNowait(db1, 'abc')

      await Db.transaction(async (db2) => {
        let rt = await inst.lockNowait(db2, 'efg')
        assert.isTrue(rt)
      }, options)

    }, options)
  })

  it('结束释放db', async function () {
    await Db.close()
  })
})


