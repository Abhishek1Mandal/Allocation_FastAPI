var flickr = require( './flickr' )
  , JSONStream = require( 'JSONStream' )

function Panda( apiKey ) {
  if ( !apiKey || typeof apiKey !== 'string' )
    throw new Error( 'must provide a valid Flickr API key' )
  this.apiKey = apiKey
}

Panda.prototype = {

  getList: function(){
    var jsonStream = JSONStream.parse( [ 'pandas', true, true, "_content" ] )
      , flickrStream 
    flickrStream = flickr.request(
      'flickr.panda.getList'
    , { api_key: this.apiKey }
    , jsonStream
    )
    return jsonStream
  }

, getPhotos: function( pandaName ) {
    var flickrStream
      , jsonStream

    jsonStream = JSONStream.parse( [ 'photos', 'photo', true ] )

    flickrStream = flickr.request(
      'flickr.panda.getPhotos'
    , {
        api_key: this.apiKey
      , panda_name: pandaName
      }
    , jsonStream
    )
    return jsonStream
  }
}

module.exports = Panda
