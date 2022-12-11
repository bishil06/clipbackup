const { Writable } = require('node:stream');

const dotenv = require('dotenv')
const { MongoClient } = require('mongodb');

dotenv.config()

const dbHost = process.env.MONGODB_DB_HOST
const dbName = process.env.MONGODB_DB_NAME;
const collectionName = process.env.MONGODB_COLLECTION_NAME;
const userID = process.env.MONGODB_USER_ID
const userPass = process.env.MONGODB_USER_PASS

async function makeSaveClipStream() {
  const dbURL = `mongodb://${userID}:${userPass}@${dbHost}:${27017}`
  const client = new MongoClient(dbURL);

  const con = client.connect()

  class SaveClipStream extends Writable {
    constructor(dbName, collectionName) {
      super({ objectMode: true })
      this.db = client.db(dbName)
      this.collection = this.db.collection(collectionName)
    }
  
    _write (clip, encoding, cb) {
      const obj = { 
        id: clip.id,
        url: clip.url,
        embedUrl: clip.embedUrl,
        broadcasterId: clip.broadcasterId,
        broadcasterDisplayName: clip.broadcasterDisplayName,
        creatorId: clip.creatorId,
        creatorDisplayName: clip.creatorDisplayName,
        video_id: clip.video_id,
        gameId: clip.gameId,
        language: clip.language,
        title: clip.title,
        views: clip.views,
        creationDate: clip.creationDate,
        thumbnailUrl: clip.thumbnailUrl,
        duration: clip.duration,
        vodOffset: clip.vodOffset
      }
  
      return this.collection.updateOne(
          { id: clip.id }, 
          { $setOnInsert: obj}, 
          {upsert: true }
        )
        .then(({modifiedCount}) => {
          if (modifiedCount !== 0) {
            console.dir(clip)
          }
        })
        .finally(cb)
    }

    _final(cb) {
      return client.close().finally(cb)
    }

    close() {
      return client.close()
    }
  }

  return con
    .then(() => new SaveClipStream(dbName, collectionName))
    .catch(err => {
      throw err
    })
}

module.exports = makeSaveClipStream