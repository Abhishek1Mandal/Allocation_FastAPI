
var s = require( './support' )
  , expect = s.expect
  , stub = s.stub
  , sandbox = require( 'sandboxed-module' )
  , httpStub = stub().returns( { end: stub() } )
  , Flickr = sandbox.require( '../lib/Flickr',{
      requires: {
        'http': { request: httpStub }
      }
    })
  , apiKey = 'lksadjflkasdjf'

describe( 'Flickr', function() {

  describe( 'constant values (Flickr[constant])', function() {

    it( 'exports an endpoint url for the http api(Flickr.httpUrl)', function() {
      expect( Flickr.httpUrl ).to.equal( 'http://api.flickr.com/services/rest' )
    })

    it( 'exports a url for the https api(Flickr.httpsUrl)', function() {
      expect( Flickr.httpsUrl )
        .to.equal( 'https://secure.flickr.com/services/rest' )
    })

  })

  describe( 'sending a request', function() {

    var methodName = 'foo'
      , opts = {
          foo: 'bar'
        , api_key: apiKey
        }
      , calledWithArgs

    Flickr.request( methodName, opts, { write: stub() } )

    calledWithArgs = httpStub.args[0]

    it( 'always requests a response in json', function() {
      expect( calledWithArgs[0] ).to.match( /&format=json/ )
    })

    it( 'correctly encodes all the params in params sent', function() {
      var opt
      for ( opt in opts ) {
        if ( opts.hasOwnProperty( opt ) ) {
          expect( calledWithArgs[ 0 ] ).to
            .contain( opt + '=' + encodeURIComponent( opts[ opt ] ) )
        }
      }
    })

    it( 'adds the method name to the url correctly', function() {
      expect( calledWithArgs[0] ).to.contain( '?method=' + methodName )
    })

    it( 'throws an error if no api_key provided in options', function() {
      var opts = { 'foo': 'bar' }
      expect( function(){ Flickr.request( methodName, opts ) }).to.throw()
    })

  })
})
