'use strict'
const crc = require('crc')
/**
 * 目前只支持 mysql 隔离等级 READ-COMMITTED 
 */

let instances = {}

class DbChannel {
  constructor(prefix) {
    if (prefix && !prefix.endsWith('_')) prefix += '_'
    this.tableName = prefix + 'channel'
    this.nowaitTableName = prefix + 'channel_nowait'

    this.selectSql = `SELECT c_uId FROM ${this.tableName} WHERE c_uId = ? FOR UPDATE`
    this.replaceSql = `REPLACE INTO ${this.tableName}(c_uId) VALUES(?)`

    this.selectSqlNowait = `SELECT cn_sName FROM ${this.nowaitTableName} WHERE {cn_sName = ?} OR... FOR UPDATE NOWAIT`
    this.replaceSqlNowait = `REPLACE INTO ${this.nowaitTableName}(cn_sName) VALUES {(?)},...`
  }

  async init(options, theoneDb) {
    if (theoneDb === undefined) theoneDb = theone.Db
    await theoneDb.transaction(async  db => {
      await db.query(
        `CREATE TABLE IF NOT EXISTS ${this.tableName} (
          c_uId int unsigned NOT NULL,
          PRIMARY KEY (c_uId)
        ) ENGINE=InnoDB COMMENT='Created by zo-theone-channel';`
      )
      await db.query(
        `CREATE TABLE IF NOT EXISTS ${this.nowaitTableName} (
          cn_sName varchar(255) NOT NULL,
          PRIMARY KEY (cn_sName)
        ) ENGINE=InnoDB COMMENT='Created by zo-theone-channel';`
      )
      await db.query(`REPLACE INTO ${this.tableName} (c_uId) VALUES {(?)},...`, [[...Array(20).keys()]]) //预插入一些记录, 防止记录较少时候锁表
    }, options)
  }

  async lock(db, channels) {
    if (!db.isBegin()) throw new Error('必须在 beginTransaction 后调用 lock')
    if (!Array.isArray(channels)) channels = [channels]
    channels = channels.map(v => crc.crc32(v.toString()))
    channels = [...new Set(channels)]
    channels.sort((a, b) => a - b)
    for (const c of channels) {
      let rt = await db.executeOne(this.selectSql, [c])
      if (!rt) {
        await db.execute(this.replaceSql, [c])
      }
    }
  }

  //nowait 使用精确锁(原字符串)   而 lock 为了效率和节约空间使用的是 crc32 整数
  async lockNowait(db, channels) {
    if (!db.isBegin()) throw new Error('必须在 beginTransaction 后调用 lock')

    if (!Array.isArray(channels)) channels = [channels]
    if (channels.length == 0) return
    channels = [...new Set(channels)]
    try {
      var rt = await db.execute(this.selectSqlNowait, [channels])
    } catch (e) {
      if (e.message.toLocaleLowerCase().includes('nowait is set')) return false   //因为 NOWAIT 返回false, 否则抛异常
      throw e
    }
    if (rt.length < channels.length) {//此时会等待 没有 nowait
      await db.execute(this.replaceSqlNowait, [channels])
    }
    return true
  }
}

module.exports = {
  //同一服务可以init 多个 channel 实例,  options.name 和 prefix 都相同 对应同一个 instance
  async init(options, prefix = 'theone', theoneDb) {
    if (typeof options == 'string') options = theone.config['databaseMap'][options]
    let name = options.name + '#' + prefix
    if (!instances[name]) {
      instances[name] = new DbChannel(prefix)
      await instances[name].init(options, theoneDb)
    }
    return instances[name]
  }
}
