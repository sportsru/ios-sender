How it works
---------------

Events -> prepared messages NSQ topic -> *ios_sender (binary&config&TLS certs) -> send messages to a lot of apps*

TODO
------

* switch on new protocol https://developer.apple.com/videos/play/wwdc2015-720/
* APNS feedback support
* tests

MAYBE
------

* switch on 3rd party lib (MAYBE https://godoc.org/github.com/timehop/apns)
* better config management? (dynamic keys)
* APNS retries on errors?

ALSO
-----

    And next year, in 2016, they may be growing up to 100 bytes.
    So if you make any assumptions about the size of device tokens in your code,
    or in your server APIs, now is a good time to revisit those assumptions.
    Large device tokens coming in 2016.

(c)https://developer.apple.com/videos/play/wwdc2015-720/

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