var fs = require('fs');
var readline = require('readline');
var google = require('googleapis');
var googleAuth = require('google-auth-library');

// If modifying these scopes, delete your previously saved credentials
// at ~/.credentials/calendar-nodejs-quickstart.json
var SCOPES = ['https://www.googleapis.com/auth/calendar'];
var TOKEN_DIR = (process.env.HOME || process.env.HOMEPATH ||
    process.env.USERPROFILE) + '/.credentials/';
var TOKEN_PATH = TOKEN_DIR + 'calendar-nodejs-quickstart.json';

// Load client secrets from a local file.
fs.readFile('client_secret.json', function processClientSecrets(err, content) {
  if (err) {
    console.log('Error loading client secret file: ' + err);
    return;
  }
  // Authorize a client with the loaded credentials, then call the
  // Google Calendar API.
  // authorize(JSON.parse(content), listEvents);
  authorize(JSON.parse(content), listCalendars);
});

/**
 * Create an OAuth2 client with the given credentials, and then execute the
 * given callback function.
 *
 * @param {Object} credentials The authorization client credentials.
 * @param {function} callback The callback to call with the authorized client.
 */
function authorize(credentials, callback) {
  console.log("Testing: ", credentials);
  var clientSecret = credentials.web.client_secret;
  var clientId = credentials.web.client_id;
  var redirectUrl = credentials.web.redirect_uris[0];
  var auth = new googleAuth();
  var oauth2Client = new auth.OAuth2(clientId, clientSecret, redirectUrl);

  // Check if we have previously stored a token.
  fs.readFile(TOKEN_PATH, function(err, token) {
    if (err) {
      getNewToken(oauth2Client, callback);
    } else {
      oauth2Client.credentials = JSON.parse(token);
      callback(oauth2Client);
    }
  });
}

/**
 * Get and store new token after prompting for user authorization, and then
 * execute the given callback with the authorized OAuth2 client.
 *
 * @param {google.auth.OAuth2} oauth2Client The OAuth2 client to get token for.
 * @param {getEventsCallback} callback The callback to call with the authorized
 *     client.
 */
function getNewToken(oauth2Client, callback) {
  var authUrl = oauth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES
  });
  console.log('Authorize this app by visiting this url: ', authUrl);
  var rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });
  rl.question('Enter the code from that page here: ', function(code) {
    rl.close();
    oauth2Client.getToken(code, function(err, token) {
      if (err) {
        console.log('Error while trying to retrieve access token', err);
        return;
      }
      oauth2Client.credentials = token;
      storeToken(token);
      callback(oauth2Client);
    });
  });
}

/**
 * Store token to disk be used in later program executions.
 *
 * @param {Object} token The token to store to disk.
 */
function storeToken(token) {
  try {
    fs.mkdirSync(TOKEN_DIR);
  } catch (err) {
    if (err.code != 'EEXIST') {
      throw err;
    }
  }
  fs.writeFile(TOKEN_PATH, JSON.stringify(token));
  console.log('Token stored to ' + TOKEN_PATH);
}

/**
 * Lists the next 10 events on the user's primary calendar.
 *
 * @param {google.auth.OAuth2} auth An authorized OAuth2 client.
 */
function listEvents(auth, id) {
  var calendar = google.calendar('v3');
  var today = new Date();
  var day = today.getDate();
  var tomo = new Date();
  tomo.setDate(new Date(day + 1));
  calendar.events.list({
    auth: auth,
    calendarId: id,
    timeMin: today.toISOString(),
    maxResults: 5,
    singleEvents: true,
   orderBy: 'startTime'
  }, function(err, response) {
    if (err) {
      console.log('The API returned an error: ' + err);
      return;
    }
    var events = response.items;
    console.log("Events: ", events);
    if(events){
      if (events.length == 0) {
        console.log('No upcoming events found.');
      } else {
        console.log('Upcoming 10 events:');
        for (var i = 0; i < events.length; i++) {
          var event = events[i];
          var start = event.start.dateTime || event.start.date;
          console.log('%s - %s', start, event.summary);
        }
      }
    } else {
      console.log("Error in fetching");
    }
  });
}


/**
 * Lists the calendars
 *
 * @param {google.auth.OAuth2} auth An authorized OAuth2 client.
 */
function listCalendars(auth) {
  var calendar = google.calendar('v3');
  calendar.calendarList.list({
    auth: auth,
    maxResults: 5    
  }, function(err, response) {
    if (err) {
      console.log('The API returned an error: ' + err);
      return;
    }
    var calendars = response.items;
    console.log("No. Of events: ", calendars.length);
    console.log("Events: ", calendars);
    if (calendars.length == 0) {
      console.log('No Calendar found.');
    } else {
      console.log('Calendars:');
      for (var i = 0; i < calendars.length; i++) {
        var cal = calendars[i];
        console.log("Calendar: ", cal);
        listEvents(auth, cal.id);
        // console.log('%s - %s', start, event.summary);
      }
    }
  });
}

/**
 * Lists the calendars
 *
 * @param {google.auth.OAuth2} auth An authorized OAuth2 client.
 */
function getCalendar(auth, id) {
  var calendar = google.calendar('v3');
  calendar.calendarList.get({
    auth: auth,
    calendarId: id   
  }, function(err, response) {
    if (err) {
      console.log('The API returned an error: ' + err);
      return;
    }
    console.log("response: ", response);
    
  });
}
