const { Writable } = require('node:stream')
const dotenv = require('dotenv')
const youtubedl = require('youtube-dl-exec')
const { createClient } = require("webdav");

dotenv.config()

const client = createClient(
    process.env.WEBDEV_URL,
    {
        username: process.env.WEBDEV_ID,
        password: process.env.WEBDEV_PASS
    }
);

module.exports = async function downloadClipVideoToWebdev(clipData) {
  return youtubedl.exec(clipData.url, {
    dumpSingleJson: true
  })
  .then(r => r.stdout)
  .then(s => JSON.parse(s))
  .then(j => j.url)
  .then(url => fetch(url))
  .then(res => res.body)
  .then(rstream => rstream.pipeTo(Writable.toWeb(client.createWriteStream(`/dlguswp23/${clipData.id}.mp4`))))
}
