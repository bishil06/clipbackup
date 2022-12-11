const { Readable } = require('node:stream');
const { pipeline: pipelinePromise } = require('node:stream/promises');

const dotenv = require('dotenv')
const { ApiClient } = require('@twurple/api');
const { ClientCredentialsAuthProvider } = require('@twurple/auth');

const makeSaveClipStream = require('./saveClipDataToDB.js')

dotenv.config()

const TWITCH_CLIENT_ID = process.env.TWITCH_CLIENT_ID
const TWITCH_CLIENT_SECRET = process.env.TWITCH_CLIENT_SECRET

const authProvider = new ClientCredentialsAuthProvider(TWITCH_CLIENT_ID, TWITCH_CLIENT_SECRET);

const apiClient = new ApiClient({ authProvider });

async function getClipAsyncIterator(userName) {
  return apiClient.users.getUserByName(userName)
    .then((user) => apiClient.clips.getClipsForBroadcasterPaginated(user.id))
}

let streamers = [
  'gosegugosegu',
  'woowakgood',
  'cotton__123',
  'jingburger',
  'lilpaaaaaa',
  'vo_ine',
  'viichan6',
  'realchunshik',
  'kwonmin98',
  'cman0327',
  'dandapbug',
  'dopamine_dr',
  '111roentgenium',
  'secretmemolee',
  'businesskim111',
  'friedshrimp70',
  'wakphago',
  'invenxd',
  'pung_sin',
  'rusuk_',
  'hikiking0',
  'dokkhye_',
  'mitsune_89',
  'bujungingan',
  'ssoph25',
  '2ducksoo',
  'carnarjungtur',
  'freeter1999',
  'nosferatu_hodd'
]

async function saveWaksClipData(streamers) {
  for (const sname of streamers) {
    console.log(sname)
    
    await getClipAsyncIterator(sname)
      .then(clipsPaginated => {
        const rstream = Readable.from(clipsPaginated)
        rstream.on('error', (err) => {
          console.error(err)
        })

        return rstream
      })
      .then(async s => pipelinePromise(s, await makeSaveClipStream()))
      .catch(err => console.error(err))
  }
}

saveWaksClipData(streamers)
