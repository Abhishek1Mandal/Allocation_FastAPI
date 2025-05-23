
var sinon = require( 'sinon' )
  , sinonChai = require( 'sinon-chai' )
  , chai = require( 'chai' )

chai.use( sinonChai )

module.exports = {
  stub: sinon.stub
, expect: chai.expect
}
