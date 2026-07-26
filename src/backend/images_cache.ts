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
import { net, protocol } from 'electron'
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
      case 'avif':
        return 'image/avif'
      case 'jpg':
      case 'jpeg':
        return 'image/jpeg'
    }
  } catch {
    // fall through to default
  }
  return 'image/jpeg'
}

const getImageFromCache = async (url: string): Promise<Response> => {
  const realUrl = decodeURIComponent(url.replace('imagecache://', ''))
  // digest of the image url for the file name
  const digest = createHash('sha256').update(realUrl).digest('hex')
  const cachePath = join(imagesCachePath, digest)
  const contentType = mimeFromUrl(realUrl)

  // If a download is already in progress, wait for it before serving to avoid
  // reading a partially-written file from cache
  if (pending.has(digest)) {
    await pending.get(digest)?.catch(() => undefined)
    if (!existsSync(cachePath)) return new Response(null, { status: 404 })
    return new Response(
      Readable.toWeb(createReadStream(cachePath)) as ReadableStream,
      {
        headers: { 'Content-Type': contentType }
      }
    )
  }

  // Serve from completed cache
  if (existsSync(cachePath)) {
    return new Response(
      Readable.toWeb(createReadStream(cachePath)) as ReadableStream,
      {
        headers: { 'Content-Type': contentType }
      }
    )
  }

  if (!realUrl.startsWith('http')) {
    return new Response(null, { status: 404 })
  }

  // Download in the background and return 404 so CachedImage falls back to
  // the real URL for this first request. Subsequent requests hit the cache.
  const download = net
    .fetch(realUrl)
    .then((response) => {
      if (!response.ok || !response.body) throw new Error('Bad response')
      const writer = createWriteStream(cachePath)
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      return pipeline(Readable.fromWeb(response.body as any), writer)
    })
    .catch(() => {
      if (existsSync(cachePath)) unlinkSync(cachePath)
    })
    .finally(() => pending.delete(digest))

  pending.set(digest, download)
  return new Response(null, { status: 404 })
}
