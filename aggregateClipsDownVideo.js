const { Writable } = require('node:stream')
const { pipeline } = require('node:stream/promises')

const dotenv = require('dotenv')
const { MongoClient } = require('mongodb');

const downloadClipVideoToWebdev = require('./downClipVideoToWebdev')

dotenv.config()

const dbName = process.env.MONGODB_DB_NAME;
const collectionName = process.env.MONGODB_COLLECTION_NAME;
const userID = process.env.MONGODB_USER_ID
const userPass = process.env.MONGODB_USER_PASS

const dbURL = `mongodb://${userID}:${userPass}@${'localhost'}:${27017}`
const client = new MongoClient(dbURL);
const db = client.db(dbName)
const collection = db.collection(collectionName)

class WebDevDownStream extends Writable {
  constructor() {
    super({ objectMode: true })
    this.count = 0
  }

  _write (clip, encoding, cb) {
    this.count += 1

    console.log(this.count, clip)

    downloadClipVideoToWebdev(clip)
      .then(() => collection.updateOne({ id: clip.id }, { $set: { isDownloaded: true }}))
      .finally(cb)
  }
}

const dbpipeline =  [
  {
      "$match": {
          "isDownloaded": {
              "$exists": false
          }
      }
  }, 
  {
      "$match": {
          "$expr": {
              "$eq": [
                  {
                      "$year": "$creationDate"
                  },
                  2021.0
              ]
          }
      }
  }, 
  {
      "$match": {
          "$expr": {
              "$lte": [
                  {
                      "$month": "$creationDate"
                  },
                  11.0
              ]
          }
      }
  }, 
  {
      "$sort": {
          "views": -1.0
      }
  }
];


client.connect()
  .then(() => {
    const cursor = collection.aggregate(dbpipeline)
    cursor.maxTimeMS(1000*100000)

    pipeline(cursor.stream(), new WebDevDownStream())
      .then(() => client.close())
  })