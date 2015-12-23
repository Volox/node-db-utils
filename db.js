'use strict';
// Load system modules

// Load modules
let Promise = require( 'bluebird' );
let debug = require( 'debug' )( 'DB' );
let MongoClient = require( 'mongodb' ).MongoClient;

// Load my modules

// Constant declaration

// Module variables declaration
let db;
let collectionMapping;

// Module functions declaration
function getCollection( name ) {
  if( db ) {
    return db.collection( collectionMapping[ name ] || name );
  } else {
    throw new Error( 'DB not available' );
  }
}
function connect( url, name, userCollectionMapping ) {
  if( db ) {
    throw new Error( 'DB already connected' );
  }


  debug( 'Opening conneciton' );
  let dbUrl = url+'/'+name;
  let connectionOptions = {
    promiseLibrary: Promise,
  };



  return MongoClient
  .connect( dbUrl, connectionOptions )
  .tap( myDB => {
    db = myDB;
    collectionMapping = userCollectionMapping || {};
  } )
  .tap( () => debug( 'Connection opened' ) );
}
function disconnect() {
  if( db ) {
    debug( 'Closing conneciton' );
    return db
    .close()
    .tap( () => debug( 'Connection closed' ) );
  } else {
    throw new Error( 'DB not available' );
  }
}


function find( collectionName, filter, fields ) {

  let collection = getCollection( collectionName );

  return collection
  .find( filter )
  .project( fields );
}
function aggregate( collectionName, pipeline ) {

  let collection = getCollection( collectionName );

  let options = {
    allowDiskUse: true,
  };

  return collection
  .aggregate( pipeline, options );
}
function insert( collectionName, data ) {
  let collection = getCollection( collectionName );

  if( !Array.isArray( data ) ) {
    return collection.insertOne( data );
  } else {
    return collection.insertMany( data );
  }
}
function update( collectionName, filter, value ) {
  let collection = getCollection( collectionName );

  return collection
  .updateOne( filter, {
    $set: value,
  } );
}





// Module class declaration

// Module initialization (at first load)

// Module exports
module.exports.connect = connect;
module.exports.open = connect;
module.exports.disconnect = disconnect;
module.exports.close = disconnect;
module.exports.getCollection = getCollection;
module.exports.get = getCollection;

module.exports.find = find;
module.exports.aggregate = aggregate;

module.exports.insert = insert;
module.exports.update = update;


//  50 6F 77 65 72 65 64  62 79  56 6F 6C 6F 78