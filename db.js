/**
 * @overview
 * @author Volox <voloxxxgmial.com>
 * @license See LICENSE file
 */

'use strict';
// Load system modules

// Load modules
let Promise = require( 'bluebird' );
let debug = require( 'debug' )( 'DB' );
let MongoClient = require( 'mongodb' ).MongoClient;

// Load my modules

// Constant declaration
/**
 * The default values for the connection
 *
 * @default { promiseLibrary: Promise }
 * @const {Object}
 */
const DEFAULT_CONNECT_OPTIONS = {
  promiseLibrary: Promise,
};
/**
 * The default values for the aggregation
 *
 * @default { allowDiskUse: true }
 * @const {Object}
 */
const DEFAULT_PIPELINE_OPTIONS = {
  allowDiskUse: true,
};

// Module variables declaration

// Module functions declaration

// Module class declaration

/**
 * Class representing the DB
 */
class DB {
  /**
   * Creates a new instance of DB
   * @constructor
   */
  constructor() {
    debug( 'DB created' );
    /**
     * @property {MongoDatabase} db The actual db returned after calling
     *                              `connect`
     */
    this.db = null;
    /**
     * @property {Map<string,string>} collectionMapping   The mapping of the
     *                                                    aliases
     */
    this.collectionMapping = new Map();



    /**
     * Alias for {@link DB#connect}
     * @memberof DB#
     * @method open
     * @alias connect
     */
    this.open = this.connect.bind( this );
    /**
     * Alias for {@link DB#disconnect}
     * @memberof DB#
     * @method close
     * @alias disconnect
     */
    this.close = this.disconnect.bind( this );
    /**
     * Alias for {@link DB#remove}
     * @memberof DB#
     * @method delete
     * @alias remove
     */
    this.delete = this.remove.bind( this );
    /**
     * Alias for {@link DB#dropDatabase}
     * @memberof DB#
     * @method dropDB
     * @alias dropDatabase
     */
    this.dropDB = this.dropDatabase.bind( this );
    /**
     * Alias for {@link DB#getCollection}
     * @memberof DB#
     * @method get
     * @alias getCollection
     */
    this.get = this.getCollection.bind( this );
    /**
     * Alias for {@link DB#getCollection}
     * @memberof DB#
     * @method g
     * @alias getCollection
     */
    this.g = this.getCollection.bind( this ); // Moar getCollection alias
    /**
     * Alias for {@link DB#getCollection}
     * @memberof DB#
     * @method c
     * @alias getCollection
     */
    this.c = this.getCollection.bind( this ); // Even mooooar
  }


  // Getters and setters
  get connection() {
    return this.db;
  }
  get collections() {
    return this.db.collections();
  }
  set mapping( collectionMapping ) {
    debug( 'Set mappings to: %j', collectionMapping );

    for( let alias in collectionMapping ) {
      if( collectionMapping.hasOwnProperty( alias ) ) {
        let realName = collectionMapping[ alias ];
        // Set the new mappings
        this.collectionMapping.set( alias, realName );
      }
    }
  }

  // Make the object iterate over the available collections
  * [Symbol.iterator]() {
    let collections = this.collectionMapping.keys();
    for( let name of collections ) {
      yield name;
    }
  }


  /**
   * Connects to the specified DB at the specified url
   * @method connect
   * @param  {string} dbUrl - The url of the db to connect to
   * @param  {string} dbName - The name of the database to use
   * @param  {Object} [options=DEFAULT_CONNECT_OPTIONS] - The options to pass
   * to the MongoClient
   * @return {Promise<MongoDatabase>}         A Promise to the database
   * @see {@link
   * http://mongodb.github.io/node-mongodb-native/2.1/api/MongoClient.html#.connect|MongoClient.connect}
   * @see {@link
   * http://mongodb.github.io/node-mongodb-native/2.1/api/Db.html|MongoDatabase}
   */
  connect( dbUrl, dbName, options ) {
    debug( 'Opening conneciton to "%s" @ %s', dbName, dbUrl );
    let dbFullUrl = dbUrl+'/'+dbName;
    options = options || {};

    let connectionOptions = Object.assign( {}, DEFAULT_CONNECT_OPTIONS, options );

    return MongoClient
    .connect( dbFullUrl, connectionOptions )
    .tap( myDB => {
      this.db = myDB;
    } )
    .tap( () => debug( 'Connection opened' ) );
  }

  /**
   * Closes the current connection
   * @method disconnect
   * @return {Promise} Promise of completed disconnect
   */
  disconnect() {
    if( this.db ) {
      debug( 'Closing conneciton' );
      return this.db
      .close()
      .tap( () => debug( 'Connection closed' ) );
    } else {
      debug( 'Connection not active' );
      return Promise.resolve();
    }
  }

  /**
   * Get the collection real name
   * @method getCollectionName
   * @param  {string} name - The name (or alias) of the collection
   * @return {string} - The real name of the collection (or the name param, if no alias is available)
   */
  getCollectionName( name ) {
    return this.collectionMapping.get( name ) || name;
  }

  /**
   * Get the specified collection
   * @method getCollection
   * @param  {string} name -  The name (or alias) of the collection
   * @return {Collection}      The MongoDB collection
   */
  getCollection( name ) {
    let collectionName = this.getCollectionName( name );

    return this.db.collection( collectionName );
  }



  // CRUD operations

  /**
   * Inserts new elements on the specified collection.
   * @method indert
   * @param  {string} name - Collection name or alias
   * @param  {(Object|Object[])} data - The data to insert, can be either an object or an Array of Objects
   * @return {Promise}            Promise of completed insert
   */
  insert( name, data ) {
    let collection = this.getCollection( name );
    debug( 'Insert on %s', name );

    if( !Array.isArray( data ) ) {
      return collection.insertOne( data );
    } else {
      return collection.insertMany( data );
    }
  }

  /**
   * Performs a find on the specified collection.
   * @method find
   * @param  {string} name -  Collection name or alias
   * @param  {Object} [filter={}] - The filter to apply to the query
   * @param  {(Object|string[])} [fields={}] - The fields to return
   * @return {Cursor}        The query Cursor of MongoDB.
   * @see {@link http://mongodb.github.io/node-mongodb-native/2.1/api/Cursor.html|Cursor} for more info.
   */
  find( name, filter, fields ) {
    let collection = this.getCollection( name );
    debug( 'Find on %s', name );

    filter = filter || {};
    debug( 'Filter: %j', filter );

    let cursor = collection
    .find( filter );

    if( fields ) {
      // Convert to a simple object.
      if( Array.isArray( fields ) ) {
        let temp = {};
        for( let field of fields ) {
          temp[ field ] = 1;
        }
        fields = temp;
      }

      // Add the project stage
      debug( 'Project fields: %j', fields );
      cursor.project( fields );
    }

    return cursor;
  }

  /**
   * Count the number of elements matching a given filter.
   * @method count
   * @param  {string} name  - Collection name or alias
   * @param  {Object} [filter={}] The filter to apply to the query
   * @return {Promise<Number>}    Promise of number of elements
   */
  count( name, filter ) {
    debug( 'Count on %s', name );
    return this.find( name, filter ).count();
  }

  /**
   * Performs an update on the specified collection.
   * @method update
   * @param  {string} name    Collection name or alias
   * @param  {Object} filter  How to select the document to update
   * @param  {Object} value   The update to perform
   * @param  {bool} [multi=false]   Update one or more elements.
   * @param  {bool} [replace=false]   Replace all data or use the "$set" modifier.
   * @return {Promise}        Promise of completed update
   */
  update( name, filter, value, multi, replace ) {
    let collection = this.getCollection( name );
    debug( 'Update on %s', name );

    debug( 'Filter: %j', filter );

    multi = multi===true? true : false; // Strictly check for boolean true to update multi documents
    replace = replace===true? true : false; // Strictly check for boolean true to replace all
    debug( 'Multi: %j', multi );
    debug( 'Replace: %j', replace );

    let updateValue = value;
    if( replace===false ) {
      updateValue = {
        $set: updateValue,
      };
    }
    debug( 'Update: %j', updateValue );

    if( multi ) {
      return collection.updateMany( filter, updateValue );
    } else {
      return collection.updateOne( filter, updateValue );
    }
  }

  /**
   * Removes documents on the specified collection.
   * @method remove
   * @param  {string} name    Collection name or alias
   * @param  {Object} filter  How to select the document to remove
   * @param  {bool} [multi=false]   Delete one or more elements.
   * @return {Promise}        Promise of completed delete
   */
  remove( name, filter, multi ) {
    let collection = this.getCollection( name );
    debug( 'Remove on %s', name );

    debug( 'Filter: %j', filter );

    multi = multi===true? true : false; // Strictly check for boolean true to update multi documents
    debug( 'Multi: %j', multi );

    if( multi ) {
      return collection.deleteMany( filter );
    } else {
      return collection.deleteOne( filter );
    }
  }




  // Other operations

  /**
   * Perform an aggregation of the specified collection.
   * @method indexes
   * @param  {string} name     Collection name or alias
   * @param  {Object[]} indexes The Array of index objects
   * @return {Promise}        Promise of completed index creation
   * @see {@link http://mongodb.github.io/node-mongodb-native/2.1/api/Collection.html#createIndexes|Collection#createIndexes} for more info.
   */
  indexes( name, indexes ) {
    let collection = this.getCollection( name );

    return collection.
    createIndexes( indexes );
  }

  /**
   * Perform an aggregation of the specified collection.
   * @method aggregate
   * @param  {string} name     Collection name or alias
   * @param  {Array<Stages>} pipeline The pipeline to execute
   * @param  {Object} [options={allowDiskUse: true}]  The options to add to the aggregation pipeline
   * @return {AggregationCursor}  The AggregationCursor of MongoDB.
   * @see {@link http://mongodb.github.io/node-mongodb-native/2.1/api/AggregationCursor.html|AggregationCursor} for more info.
   */
  aggregate( name, pipeline, options ) {
    let collection = this.getCollection( name );

    let pipelineOptions = Object.assign( {}, DEFAULT_PIPELINE_OPTIONS, options );

    return collection
    .aggregate( pipeline, pipelineOptions );
  }

  /**
   * Drops the specified collection.
   * @method drop
   * @param  {string} name     Collection name or alias
   * @return {Promise}        Promise of completed drop
   */
  drop( name ) {
    let collection = this.getCollection( name );

    return collection
    .drop();
  }

  /**
   * Drops the current database.
   * @method dropDatabase
   * @return {Promise}        Promise of completed dropDatabase
   */
  dropDatabase() {
    return this.db
    .dropDatabase();
  }

}

// Module initialization (at first load)

// Module exports
module.exports = new DB();


//  50 6F 77 65 72 65 64  62 79  56 6F 6C 6F 78