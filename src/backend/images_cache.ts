import {
  existsSync,
  createWriteStream,
  createReadStream,
  mkdirSync,
  unlinkSync
} from 'graceful-fs'
import { Readable } from 'stream'
import { pipeline } from 'stream/promises'
import { createHash } from 'crypto'
import { join } from 'path'
import axios from 'axios'
import { protocol } from 'electron'
import { appFolder } from './constants/paths'

const imagesCachePath = join(appFolder, 'images-cache')

export const initImagesCache = () => {
  // make sure we have a folder to store the cache
  if (!existsSync(imagesCachePath)) {
    mkdirSync(imagesCachePath)
  }

  // use a fake protocol for images we want to cache
  protocol.handle('imagecache', (request) => {
    return getImageFromCache(request.url)
  })
}

const pending = new Map<string, Promise<void>>()

function mimeFromUrl(url: string): string {
  try {
    const ext = new URL(url).pathname.split('.').pop()?.toLowerCase()
    switch (ext) {
      case 'gif':
        return 'image/gif'
      case 'png':
        return 'image/png'
      case 'webp':
        return 'image/webp'
    }
  } catch {
    // fall through to default
  }
  return 'image/jpeg'
}

const serveFromDisk = (cachePath: string, contentType: string) =>
  new Response(Readable.toWeb(createReadStream(cachePath)) as ReadableStream, {
    headers: { 'Content-Type': contentType }
  })

const getImageFromCache = async (url: string): Promise<Response> => {
  const realUrl = decodeURIComponent(url.replace('imagecache://', ''))
  // digest of the image url for the file name
  const digest = createHash('sha256').update(realUrl).digest('hex')
  const cachePath = join(imagesCachePath, digest)
  const contentType = mimeFromUrl(realUrl)

  // wait for the pending download, otherwise we might serve a half-written file
  if (pending.has(digest)) {
    await pending.get(digest)?.catch(() => undefined)
    if (!existsSync(cachePath)) return new Response(null, { status: 404 })
    return serveFromDisk(cachePath, contentType)
  }

  if (existsSync(cachePath)) {
    return serveFromDisk(cachePath, contentType)
  }

  if (!realUrl.startsWith('http')) {
    return new Response(null, { status: 404 })
  }

  // 404 here makes CachedImage fall back to the real url until it's cached
  const download = axios({
    method: 'get',
    url: realUrl,
    responseType: 'stream'
  })
    .then((response) => pipeline(response.data, createWriteStream(cachePath)))
    .catch(() => {
      if (existsSync(cachePath)) unlinkSync(cachePath)
    })
    .finally(() => pending.delete(digest))

  pending.set(digest, download)
  return new Response(null, { status: 404 })
}
