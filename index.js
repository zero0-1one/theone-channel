'use strict'
const crc = require('crc')
/**
 * 目前只支持 mysql 隔离等级 READ-COMMITTED 
 */

let instances = {}

class DbChannel {
  constructor(prefix) {
    if (prefix && !prefix.endsWith('_')) prefix += '_'
    this.tableName = prefix + '_channel'
    this.selectSql = `SELECT c_uId FROM ${this.tableName} WHERE c_uId = ? FOR UPDATE`
    this.replaceSql = `REPLACE INTO ${this.tableName}(c_uId) VALUES(?)`
  }

  async init(options, theoneDb) {
    if (theoneDb === undefined) theoneDb = theone.Db
    await theoneDb.transaction(async  db => {
      await db.query(
        `CREATE TABLE IF NOT EXISTS ${this.tableName} (
          c_uId int unsigned NOT NULL,
          PRIMARY KEY (c_uId)
        ) ENGINE=InnoDB;`
      )
      await db.query(`REPLACE INTO ${this.tableName} (c_uId) VALUES(1),(2)`) //防止单行记录锁表,先支持插入2条记录
    }, options)
  }

  async lock(db, channels) {
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