'use strict';

let db = require( './db' );

let map = {
  a: 'b',
  c: 'd',
};

db.mapping = map;

for( let name of db ) {
  let realName = db.getCollectionName( name )
  console.log( '%s => %s', name, realName );
}

let data = [
  { id: '1' },
  { id: '2' },
  { id: '2' },
  { id: '5' },
];
const collection = 'a';


db
.open( 'mongodb://localhost', 'Test' )
.then( ()=> {
  return db.insert( collection, data );
} )
.then( ()=> {
  // Update 1
  return db.update( collection, { id: '2' }, { name: 'volo', culo: true } );
} )
.then( ()=> {
  // Update multi
  return db.update( collection, { name: { $ne: 'volo' } }, { name: 'no-volo' }, true );
} )
.then( ()=> {
  // Update REPLACE
  return db.update( collection, { id: '2' }, { name: 'volo' }, false, true );
} )
.then( ()=> {
  // Find+project
  return db
  .find( collection, { id: '2' }, { _id: 0 } )
  .toArray()
  .then( docs => {
    console.log( 'Docs[%d] ', docs.length );
    console.log( 'Docs[0] ', docs[ 0 ] );
  } );
} )
.then( ()=> {
  // Find+project
  return db
  .find( collection, { id: '2' }, [ 'id' ] )
  .toArray()
  .then( docs => {
    console.log( 'Docs[%d] ', docs.length );
    console.log( 'Docs[0] ', docs[ 0 ] );
  } );
} )
.then( ()=> {
  // Count
  return db
  .count( collection, { id: '2' } )
  .then( num => {
    console.log( 'Num: %d===1', num );
  } );
} )
.then( ()=> {
  let pipeline = [
    {
      $group: {
        _id: '$name',
        count:  { $sum: 1 },
      }
    },
  ];
  // Aggregate
  return db
  .aggregate( collection, pipeline )
  .toArray()
  .then( d => {
    console.log( 'Data: %j', d );
  } );
} )


.then( ()=> {
  return db.remove( collection, {}, true );
} )
.then( ()=> {
  return db.drop( collection );
} )
.then( ()=> {
  return db.dropDB();
} )
.finally( db.close );