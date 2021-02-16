# producer-consumer-server
POSIX thread producer and consumer server
**make &amp;&amp; ./pcserver && ./producer localhost port N rate bad** 
where **localhost** for hostname, **port** for port number, **N** is number of producers, **rate** is random floating point number used to randomly (Poisson distribution) sleep producer generation, **bad** is percentage[0-100] of producers used to sleep to imitate hanging process
