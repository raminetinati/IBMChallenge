var app = require('http').createServer(handler);
var io = require('socket.io')(app);
var fs = require('fs');
var _ = require('underscore');
//var config = require('./config');
var Twit = require('twit')
var fs = require('fs');
var natural = require('natural');
var stopWords = require('stopwords').english;


//set the http
app.listen(9001);

//database stuff
var max_id = 0;
var since_id = 0;

var blacklist = {};

//get the since_id from file
    fs.readFile( __dirname + '/since_id.config', function (err, data) {
  if (err) {
    throw err; 
  }
    console.log("LOADING SINCE_ID:"+data.toString());
    since_id = parseInt(data.toString());
});


//get the since_id from file
    fs.readFile( __dirname + '/blacklisted_words', function (err, data) {
  if (err) {
    throw err; 
  }
    console.log("LOADING BlackList");
    var blacklist_words = parseInt(data.toString().split(" "));
    for(word in blacklist_words){

      blacklist[blacklist_words[word]] = 1;

    }
});



var mongoose = require('mongoose');
mongoose.connect('mongodb://localhost/twitter_nyc');
var db = mongoose.connection;
db.on('error', console.error.bind(console, 'connection error:'));
db.once('open', function (callback) {
    console.log("connected to database");
});

var tweetDoc = new mongoose.Schema({
  source: String,
  message: String,
  date: Date
});


var Message = mongoose.model('Message', tweetDoc); 



function showErr (e) {
    console.error(e, e.stack);
}

function handler (req, res) {
    res.writeHead(200);
    res.end("");
}

var filter = {
    "filter": false,
}; // global filter state, and default

io.on('connection', function (socket) {
    socket.emit("filter", filter); // emit the current state to this client
    // receive a filter update, combine it and send to ALL clients
    socket.on('define_data', function (data) {
        console.log("Got Website Response:", data.define_data);
        _.extend(filter, data);
        //console.log("emitting filter:", filter); 
        updateDatabaseFromWebsite(data)
        
    });


     socket.on('load_data', function (data) {
        console.log("Loading New Application User");
        //console.log("emitting filter:", filter); 
        loadDatabaseData(socket);        
    });


});

    var T = new Twit({
    consumer_key:         'xBKmTz61D88R9axzb67LIQ'
  , consumer_secret:      '4dVqfkB92gam2s9ds2U9Ux9xFJH7Y26HQWNojJwyU'
  , access_token:         '41944067-tGaOU7HxzbDdGOLm89fT4az6tYQ9q0fFwEwdq1wfh'
  , access_token_secret:  'X3TvO96cU0P8X45dLyE1VejtbsCh43qMTzcYBssjPQk'
  });



function loadDatabaseData(socket){
    var response = [];
    Message.find(function (err, responses) {
    if (err) return console.error(err);
     //console.log(responses);
     try{
          socket.emit("historic_data", responses.slice((responses.length-1000), (responses.length-1)));
        }catch(e){
          socket.emit("historic_data", responses);
      }
        //now for some graph processing
        //createHistoricGraph(responses.slice((responses.length-50), (responses.length-1)));
    })

}


function updateDatabaseFromWebsite(data){

            var today = new Date();
            
            var doc = new Message({
                        source: "website",
                        message: data.define_data,
                        date: today
                        
                });

                doc.save(function(err, doc) {
                if (err) return console.error(err);
                //console.dir(thor);
                });
                 io.emit("realtime_data", doc)
                 updateGraphFromWebsite(data);
     

};



function calltwitter(sinceID){

    console.log("calling Twitter")

    //first call
    if(sinceID==0){
        T.get('search/tweets', { q: 'nyc OR New York OR Manhatten OR New York City OR brooklyn OR Queens OR Bronx', count: 100 }, function(err, data, response) {
        //console.log(data)
        max_id = data.search_metadata.max_id;
        updateDatabaseWithTweets(since_id,max_id,data);
        });
    }
    //calll after since ID has been set
    else{
         T.get('search/tweets', { q: 'nyc OR New York OR Manhatten OR New York City OR brooklyn OR Queens OR Bronx', since_id: sinceID, count: 100}, function(err, data, response) {
        //console.log(data)
        max_id = data.search_metadata.max_id;
        updateDatabaseWithTweets(since_id,max_id,data);

        });
    }   

    //geocode: '40.7127,74.0059,50mi'
    


};

function updateDatabaseWithTweets(since_id_local,max_id,data_rec){

    console.log("Updating Database")


    console.log("since_ID: "+since_id_local);
    console.log("max_ID: "+max_id);

    //console.log(data_rec);
    if(max_id>since_id_local){
            for(var status in data_rec.statuses){

            	//emit to stream
            	io.emit('tweets',data_rec.statuses[status]);

                //create document and save
                var doc = new Message({
                        source: "twitter",
                        message: data_rec.statuses[status].text,
                        date: data_rec.statuses[status].created_at
                        
                });

                doc.save(function(err, doc) {
                if (err) return console.error(err);
                //console.dir(thor);
                });



                // console.log("-----")
                // console.log(data_rec.statuses[status].text)
                // console.log(data_rec.statuses[status].created_at)
                // console.log("-----")
            }
            
            console.log("Added New Items: "+data_rec.statuses.length);
			//emit to real-time
			if(data_rec.statuses.length>0){
				io.emit("realtime_data", data_rec.statuses)
        //update graph as well
        //updateGraphFromTwitter(data_rec.statuses);
		}
      }

      //set the new since_id
      since_id = max_id;

      //store the new sinceId to file
        fs.writeFile( __dirname + '/since_id.config', since_id, function(err) {
        if(err) {
            console.log(err);
         } else {
            //console.log("The file was saved!");
        }



}); 
};






//need to do some processing for the graph edges.
function updateGraphFromWebsite(data){

  console.log("Updating Graph with Website Data");
  var graph = viva.Graph.graph();
    var matchedWords = {};

   tokenizer = new natural.TreebankWordTokenizer();

      var msg = data.define_data;
     // console.log(msg);
      //remove stop word.
      //console.log("BEFORE:"+msg);

      for(sw in stopWords){
        msg = msg.replace(sw, "");
        msg = replaceUnknownTerms(msg);
      }

      var tokens = tokenizer.tokenize(msg);

            for(token_i in tokens){
                    
                    if(tokens[token_i] in matchedWords){

                      matchedWords[tokens[token_i]] = matchedWordstokens[tokens[token_i]] + 1;
                    }else{
                      matchedWords[tokens[token_i]] = 1;

                    }
                  }

  io.emit("realtime_edge_data", matchedWords);
}

function replaceUnknownTerms(msg){

  msg = msg.replace("http","").replace("#","").replace(":","").replace("//","");
  return msg;


}


function messageInBlackList(message){

  msg = message.split(" ")
  for(word in message){

    if(message[word] in blacklist){
      return false;
    }

  }
  return true;



}

var interval = setInterval(function(){calltwitter(since_id)}, 10000);


