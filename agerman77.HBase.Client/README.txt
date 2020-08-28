
Author: Alex PG
Email: agerman77@gmail.com

Project Name: agerman77.HBase.Client
Dependency: Microsoft.HBase.Client 0.4.3.0

Description:

agerman77.HBase.Client is a client library for using HBase Server (tested with version 2.3.1). It uses Microsoft.HBase.Client as base library.

Some of the features that have been added:
	-	Namespace creation, deletion
	-	Scanner with multiple filters (including multiple column filters)
	-	AutoIncrement keys for tables

The AutoIncrement feature is on the agerman77.HBase.AutoIncrement.Client library project.

Things to take into account: 
	-	Authentication is not implemented on this library *. 
	-	It has been tested on a local server. 
	-	REST service must be started on the HBase server.

	*although the constructor for HBaseConnection requires username and password, it's not really being used, you can use two empty strings as parameters.

Use at your own risk.

