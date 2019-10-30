## ACL Configure
```
Attention: Acl Type Change, change `pub =1, sub=2`  to `sub =1, pub=2`
```
#### The ACL rules define:
~~~
Allow | type | value | pubsub | Topics
~~~
#### ACL Config
~~~
## type clientid , username, ipaddr
##sub 1 ,  pub 2,  pubsub 3
## %c is clientid , %u is username
allow      ip          127.0.0.1   2     $SYS/#
allow      clientid    0001        3     #
allow      username    admin       3     #
allow      username    joy         3     /test,hello/world 
allow      clientid    *           1     toCloud/%c
allow      username    *           1     toCloud/%u
deny       clientid    *           3     #
~~~

~~~
#allow local sub $SYS topic
allow      ip          127.0.0.1   1    $SYS/#
~~~
~~~
#allow client who's id with 0001 or username with admin pub sub all topic
allow      clientid    0001        3        #
allow      username    admin       3        #
~~~
~~~
#allow client with the username joy can pub sub topic '/test' and 'hello/world'
allow      username    joy         3     /test,hello/world 
~~~
~~~
#allow all client pub the topic toCloud/{clientid/username}
allow      clientid    *         2         toCloud/%c
allow      username    *         2         toCloud/%u
~~~
~~~
#deny all client pub sub all topic
deny       clientid    *         3           #
~~~
Client match acl rule one by one
~~~
          ---------              ---------              ---------
Client -> | Rule1 | --nomatch--> | Rule2 | --nomatch--> | Rule3 | --> 
          ---------              ---------              ---------
              |                      |                      |
            match                  match                  match
             \|/                    \|/                    \|/
        allow | deny           allow | deny           allow | deny
~~~
