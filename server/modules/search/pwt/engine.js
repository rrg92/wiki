const _ = require('lodash')
const stream = require('stream')
const Promise = require('bluebird')
const pipeline = Promise.promisify(stream.pipeline)

const els = require(__dirname+'/../elasticsearch/engine.js');

var PwtSearch = {};

Object.setPrototypeOf(PwtSearch, els);



PwtSearch.init = async function() {
	els.config = this.config
	WIKI.logger.info(`(SEARCH/PWT) Initializing client...`)
	const { Client: Client7 } = require('elasticsearch7')
	this.client = new Client7({
	  nodes			: this.config.hosts.split(',').map(_.trim)
	  ,sniffOnStart	: false
	  ,name			: 'wiki-js-pwt'
	})
	
	els.client = this.client;
	
	WIKI.logger.info(`(SEARCH/PWT) Creating indexes...`)
    await this.createIndex()
	
	//this.client = els.client;
	WIKI.logger.info(`(SEARCH/PWT) Init Done!...`)
	
	this.searchStats = true;
}

PwtSearch.createPageIndex = async function(){
	const indexExists = await this.client.indices.exists({ index: this.config.indexName })

			if(indexExists.body){
				return;
			}

			WIKI.logger.info(`(SEARCH/PWT) Creating index...`)

			var IndexMapping = JSON.parse(this.config.mappings);
			var idxBody = IndexMapping
			  
			var mappings = idxBody;

			await this.client.indices.create({
				index: this.config.indexName,
				body: {
				  mappings: mappings
				  ,settings: {
						analysis:{
								analyzer:{default: {type: this.config.analyzer}
							}
						}
					}
				}
			})
}

PwtSearch.createStatIndex = async function(){
	// Check search statistics search index.
	let statIndexName = this.config.searchIndexName ;
	const searchIndexEx = await this.client.indices.exists({ index: statIndexName })

	if(searchIndexEx.body){
	   return;
	}
	
	WIKI.logger.info(`(SEARCH/PWT) Creating index search: ${statIndexName}`);
	await this.client.indices.create({
		index: statIndexName,
		body: {
		  mappings: {
			properties:{
				terms		: { type:"text", fields:{ kw:{type:"keyword"}} }
				,results	: {type:"object"}
				,extra		: {type:"object"}
				,user		: {type:"object"}
				,ts			: {type:"date"}
			}
		  }
		  ,settings: {
				analysis:{
						analyzer:{default: {type: this.config.analyzer}
					}
				}
			}
		}
	})
}

PwtSearch.createIndex = async function(){
	
	try {
		this.createPageIndex();
		this.createStatIndex();
    } catch (err) {
      WIKI.logger.error(`(SEARCH/PWT) Index Create Error`)
      WIKI.logger.error(err);
    }
}

PwtSearch.indexStat = async function(content){
    let statIndexName = this.config.searchIndexName ;
	
	await this.client.index({
      index: statIndexName,
      type: '_doc',
      //id: page.hash,
      body: {
		terms		: content.terms
		,results	: content.results
		,extra		: content.extra
		,user		: content.user
		,ts			: (new Date()).getTime()
      },
      refresh: true
    })
}


PwtSearch.query = async function(q,o) {
	var FinalResults;
	var SearchTerms;
	var PwtSearchOpts;
	
	try {
		var SearchResult = {};	
		if(q.startsWith('#PWTJSON#')){
			WIKI.logger.info(`(SEARCH/PWT) Doing custom pwt search...`);
			var JsonText = q.replace(/^\#PWTJSON\#:/,'')
			PwtSearchOpts = JSON.parse(JsonText);
			SearchTerms = PwtSearchOpts.q;
			
			/*{
				tags:[""]
				paths:["/abc/*"]
				index:{}
				q: "Generic query..."
			 }
			*/
			
			var SearchFilter = [];
			var MustQuery = [];
			var Indices = PwtSearchOpts.index;

			if(PwtSearchOpts.tags && PwtSearchOpts.tags.length > 0){
				SearchFilter.push({
					terms:{
						'tags.kw':PwtSearchOpts.tags
					}
				})
			}
			
			if(PwtSearchOpts.q){
				MustQuery.push({
					simple_query_string:{
						query				: `*${PwtSearchOpts.q}*`
						,fields				: ['title^20', 'description^3', 'tags^8', 'content^1']
						,default_operator	: 'and'
						,analyze_wildcard	: true
					}
				})
			}
			
			
			
			
			var QuerySearch = {
				bool:{
					must	: MustQuery
					,filter	: SearchFilter
				}
			}
			
			if(!Indices){
				Indices = {
					'wiki':{
						boost: 2
					}
				}
			}
			
			var IndexNameList = [];
			var IndexBoost = [];
			var BoostSetting;
			var IndexOptions;
			for(IndexName in Indices){
				IndexNameList.push(IndexName);
				IndexOptions = Indices[IndexName];
				
				BoostSetting = {};
				BoostSetting[IndexName] = IndexOptions.boost;
				
				IndexBoost.push(BoostSetting)
			}
			
			var elsOptions = {
					index: IndexNameList.join(',')
					,body: {
						   query	: QuerySearch
						  ,indices_boost:IndexBoost
						  ,from		: 0
						  ,size		: 50
						  ,_source	: ['title', 'description', 'path', 'locale', 'tags']
					}
			}
			
			console.log(JSON.stringify(elsOptions));
			
			SearchResult = await this.client.search(elsOptions)
			FinalResults = {
				results: _.get(SearchResult, 'body.hits.hits', []).map(r => ({
										  id			: r._id,
										  locale		: r._source.locale,
										  path			: r._source.path,
										  title			: r._source.title,
										  description	: r._source.description
										  ,tags			: r._source.tags
										  ,score		: r._score
										})
					)
				,suggestions: [] //_.reject(_.get(results, 'suggest.suggestions', []).map(s => _.get(s, 'options[0].text', false)), s => !s)
				,totalHits: _.get(SearchResult, 'body.hits.total.value', _.get(SearchResult, 'body.hits.total', 0))
			}
		}
		else {
			WIKI.logger.info(`(SEARCH/PWT) Doing elastic query...`);
			FinalResults = await els.query(q,o);
			SearchTerms = q;
		}
		
		var ContextUser = o._context.req.user;
		
		let statContent = {
			terms		: SearchTerms
			,results	: FinalResults
			,extra		: {
					rawSearch: PwtSearchOpts
				}
			,user		: {
				email: ContextUser.email
			}
		}
		
		try {
			await PwtSearch.indexStat(statContent);
		} catch (err){
			WIKI.logger.err('Indexing search results resulted in errors');
			WIKI.logger.err(err);
		}
		
		//console.log(FinalResults);
		return FinalResults;
	} catch(err) {
		console.log(err);
	}
}







/*
PwtSearch.init = function(){
	console.log('Inicialiando PWT CLASS');
	els.init();
	console.log('Iniciaido...');
}
*/


/* global WIKI */
module.exports = PwtSearch;