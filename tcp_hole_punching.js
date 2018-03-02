const express = require('express')
const app = express()

app.get('/', (req, res) => res.send('Hello World!'))

app.listen(3000, () => console.log('Example app listening on port 3000!'))
/* To add code here.. the code shoult print external ip and port instead of 3000 (which is internal port) */
