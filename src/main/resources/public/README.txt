It is recommended to serve this demo from a server rather than the file system; in particular if you are using an older browser (IE7/IE8) then KeyLines will not load properly when served from the file system.

If you are using NodeJS as your server, just run this file 'node server.js'

Running server.js has a dependency on the express module being available, which can be installed (once node is installed) by running

 >  npm install express

If you can't run Node or express just use a simple python server
    
 > python -m SimpleHTTPServer 8080

Then you can navigate to http://localhost:8080 in your web browser to see the demo running

Note: If the demo relies on a database (e.g. the Neo4j or Titan demos) then you will have to setup your database and load the data in order for it to work.
