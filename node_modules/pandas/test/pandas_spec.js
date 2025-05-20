
var support = require( './support' )
  , sandbox = require( 'sandboxed-module' )
  , expect = support.expect
  , stream = require( 'stream' ).Stream
  , stub = support.stub
  , flickrStub = stub()
  , parsedJsonPipeStub = stub()
  , jsonStreamStub = stub().returns( parsedJsonPipeStub )
  , Panda = sandbox.require( '../lib/pandas', {
      requires: {
        './flickr': { request: flickrStub }
      , 'JSONStream': { parse: jsonStreamStub }
      }
    })


describe( 'Panda', function() {

  beforeEach( function() {
    flickrStub.reset()
    jsonStreamStub.reset()
    parsedJsonPipeStub.reset()
  })

  var apiKey = 'laksdjlksdafj'
    , panda  = new Panda( apiKey )

  it( 'throws an error if api_key not provided/falsey', function(){
    expect( Panda ).to.throw( /must provide a valid Flickr API key/i )
  })

  it( 'throws an error if api_key is not a string', function() {
    expect( function(){ new Panda( 3214 ) } )
      .to.throw( /must provide a valid Flickr API key/i )
  })

  it( 'stores the api key', function() {
    expect( panda.apiKey ).to.equal( apiKey )
  })

  describe( '#getList', function() {

    var flickrStream

    beforeEach( function() {
      flickrStream = panda.getList()
    })

    it( 'passes along the panda\'s api_key in an options object', function() {
      expect( flickrStub.args[0][1] ).to.have.ownProperty( 'api_key' )
    })

    it( 'requests a response for the flickr.panda.getList', function() {
      expect( flickrStub ).to.have.been.calledWith( 'flickr.panda.getList' )
    })

    it( 'parses out pandas', function() {
      expect( jsonStreamStub ).to.have.been
        .calledWith( [ 'pandas', true, true, '_content' ] )
    })

    it( 'returns the JSONStream', function() {
      expect( flickrStream ).to.equal( parsedJsonPipeStub )
    })

  })

  describe( '#getPhoto', function() {

    var photoStream

    beforeEach( function() {
      photoStream = panda.getPhotos( 'pan' )
    })

    it( 'passes along the panda\'s api_key in an options object', function() {
      expect( flickrStub.args[0][1] ).to.have.ownProperty( 'api_key' )
    })

    it( 'passes along the panda name to the flickr request', function() {
      expect( flickrStub.args[0][1] ).to.have.ownProperty( 'panda_name' )
    })

    it( 'requests a response for the flickr.panda.getPhoto', function() {
      expect( flickrStub ).to.have.been.calledWith( 'flickr.panda.getPhotos' )
    })

    it( 'creates a JSON stream that listens for photos > photo', function() {
      expect( jsonStreamStub ).to.have
        .been.calledWith([  'photos', 'photo', true ])
    })

    it( 'returns the JSON stream that listens for photos', function() {
      expect( photoStream ).to.equal( parsedJsonPipeStub )
    })

  })

})
