Pandas
====
Streaming API for Flickr's [Panda API][pandas-api]

## Usage

### Panda( apiKey )

You'll need to register an API key with flickr. This is *not* your secret key.

Calling 'Panda' without an `apiKey` or a non-string `apiKey` will throw an
 Error

Usage:

```
var Panda = require( 'pandas' )
  , panda = new Panda( 'myApiKey' )
```

### getList()

Returns a `ReadableStream` that emits a `data` event for every panda name.

Usage:

```
var pandaNameStream = panda.getList()
pandaNameStream.on( 'data', function( data ) {
  console.log( data )
})

```

### getPhotos( pandaName )

Returns a `ReadableStream` that emits a `data` event for every photo the panda
 tells us about

```
var pandaPhotoStream = panda.getPhoto( 'example panda' )
pandaPhotoStream.on( 'data', function( photo ) {
  //if the panda has 10 photos, this will be called 10 times
  console.log( JSON.stringify( photo ) )
})
```

See the `example.js` file for an example of it in action. You'll need to create
 your own `api_settings.json` file with a key called `api_key` and a value
 which is your Flickr API key

[pandas-api]:http://code.flickr.com/blog/2009/03/03/panda-tuesday-the-history-of-the-panda-new-apis-explore-and-you/
