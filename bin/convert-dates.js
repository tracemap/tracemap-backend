// This script takes legacy tracemap gen logs and transforms the millisecond-based timestamp
// into a iso date string, saving it under the new key `datetime`.
// The old key `time_str` is deleted in the process.

const fs = require('fs')
const eol = require('os').EOL
const file = '/path/to/logfile/with/old/timestamp/format.jsonl'
const data = fs.readFileSync(file, {encoding: 'utf8'}).trim().split(eol)

data.forEach(line => {
	let log = JSON.parse(line)
	datetime = new Date(parseInt(log['time_str']) * 1000)
	delete log['time_str']
	log['datetime'] = datetime.toISOString()
	console.log(JSON.stringify(log))
});

