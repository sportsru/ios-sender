How it works
---------------

Events -> prepared messages NSQ topic -> *ios_sender (binary&config&TLS certs) -> send messages to a lot of apps*

TODO
------
* switch on 3rd party lib (MAYBE https://godoc.org/github.com/timehop/apns)
* APNS feedback support
* code refactor & tests
* better config management?
* APNS retries on errors?

KNOWN BUGS
----------
* on wrong pem paths/files in config don't crash or create app statistic with error

Some notes about problem with APNS response system
-------------------------------------------------
http://redth.codes/the-problem-with-apples-push-notification-ser/

Apple docs:
---------------------
* https://developer.apple.com/library/ios/documentation/NetworkingInternet/Conceptual/RemoteNotificationsPG/Introduction.html
* https://developer.apple.com/library/ios/documentation/NetworkingInternet/Conceptual/RemoteNotificationsPG/Chapters/CommunicatingWIthAPS.html