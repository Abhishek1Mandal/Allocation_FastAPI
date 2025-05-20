
var Panda = require( './lib/pandas' )
  , apiKey = require( './api_settings' ).api_key
  , panda = new Panda( apiKey )
  , list = panda.getList()

list.on( 'data', function( data ) {
  console.log( JSON.stringify( data ) )
  var photoStream = panda.getPhotos( data )
  photoStream.on( 'data', function( data ) {
    console.log( JSON.stringify( data ) )
  })
})
