'use strict'
const crc = require('crc')
/**
 * 目前只支持 mysql 隔离等级 READ-COMMITTED 
 */

let instances = {}

class DbChannel {
  constructor(prefix) {
    if (prefix && !prefix.endsWith('_')) prefix += '_'
    this.tableName_crc32 = prefix + 'channel_crc32'
    this.tableName_string = prefix + 'channel_string'

    this.selectSql_crc32 = `SELECT c_uId FROM ${this.tableName_crc32} WHERE c_uId = ? FOR UPDATE`
    this.replaceSql_crc32 = `REPLACE INTO ${this.tableName_crc32}(c_uId) VALUES(?)`
    this.selectSql_string = `SELECT cn_sName FROM ${this.tableName_string} WHERE cn_sName = ? FOR UPDATE`
    this.replaceSql_string = `REPLACE INTO ${this.tableName_string}(cn_sName) VALUES(?)`

    this.selectSql_crc32_nowait = `SELECT c_uId FROM ${this.tableName_crc32} WHERE {c_uId = ?} OR... FOR UPDATE NOWAIT`
    this.replaceSql_crc32_nowait = `REPLACE INTO ${this.tableName_crc32}(c_uId) VALUES {(?)},...`
    this.selectSql_string_nowait = `SELECT cn_sName FROM ${this.tableName_string} WHERE {cn_sName = ?} OR... FOR UPDATE NOWAIT`
    this.replaceSql_string_nowait = `REPLACE INTO ${this.tableName_string}(cn_sName) VALUES {(?)},...`
  }

  async init(options, theoneDb) {
    if (theoneDb === undefined) theoneDb = theone.Db
    await theoneDb.transaction(async  db => {
      await db.query(
        `CREATE TABLE IF NOT EXISTS ${this.tableName_crc32} (
          c_uId int unsigned NOT NULL,
          PRIMARY KEY (c_uId)
        ) ENGINE=InnoDB COMMENT='Created by zo-theone-channel';`
      )
      await db.query(
        `CREATE TABLE IF NOT EXISTS ${this.tableName_string} (
          cn_sName varchar(255) NOT NULL,
          PRIMARY KEY (cn_sName)
        ) ENGINE=InnoDB COMMENT='Created by zo-theone-channel';`
      )
      // await db.query(`REPLACE INTO ${this.tableName} (c_uId) VALUES {(?)},...`, [[...Array(20).keys()]]) //预插入一些记录, 防止记录较少时候锁表
    }, options)
  }

  //默认 type='crc32',  lock 为了效率和节约空间使用的是 crc32, 对精准要求高也可以使用 string
  async lock(db, channels, type = 'crc32') {
    if (!db.isBegin()) throw new Error('必须在 beginTransaction 后调用 lock')
    if (!Array.isArray(channels)) channels = [channels]
    if (type == 'crc32') channels = channels.map(v => crc.crc32(v.toString()))
    channels = [...new Set(channels)]
    channels.sort((a, b) => a - b)
    for (const c of channels) {
      let rt = await db.executeOne(type == 'crc32' ? this.selectSql_crc32 : this.selectSql_string, [c])
      if (!rt) {
        await db.execute(type == 'crc32' ? this.replaceSql_crc32 : this.replaceSql_string, [c])
      }
    }
  }

  // 默认 type='string',   nowait 为了精确默认使用(原字符串), 对精准要求不高也可以使用crc32
  async lockNowait(db, channels, type = 'string') {
    if (!db.isBegin()) throw new Error('必须在 beginTransaction 后调用 lock')

    if (!Array.isArray(channels)) channels = [channels]
    if (channels.length == 0) return
    if (type == 'crc32') channels = channels.map(v => crc.crc32(v.toString()))
    channels = [...new Set(channels)]
    try {
      var rt = await db.execute(type == 'crc32' ? this.selectSql_crc32_nowait : this.selectSql_string_nowait, [channels])
    } catch (e) {
      if (e.message.toLocaleLowerCase().includes('nowait is set')) return false   //因为 NOWAIT 返回false, 否则抛异常
      throw e
    }
    if (rt.length < channels.length) {//此时会等待 没有 nowait
      await db.execute(type == 'crc32' ? this.replaceSql_crc32_nowait : this.replaceSql_string_nowait, [channels])
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
