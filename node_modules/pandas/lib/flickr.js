
var http = require( 'http' )
  , url = require( 'url' )

function encodeOpts( opts ) {
  return '&' + Object.keys( opts ).map( function( key ) {
    return key + '=' + encodeURIComponent( opts[ key ] )
  }).join( '&' ) + '&format=json' + '&nojsoncallback=1'
}

var Flickr = {
  request : function( method, params, outStream ) {
    if ( !params || !params.api_key )
      throw new Error( 'valid Flickr API key needed!' )
    var req = http.request( Flickr.httpUrl +
      '?method=' + encodeURIComponent( method ) +
      encodeOpts( params ), function( res ) {
        res.pipe( outStream )
      })
    req.end()
    return req
  }
}

// api constants
Flickr.httpUrl = 'http://api.flickr.com/services/rest'
Flickr.httpsUrl = 'https://secure.flickr.com/services/rest'

module.exports = Flickr
