Degoo are a cloud storage provider based in Sweden, who provide fairly goo phone apps and web interface along with affordable plans, of up 10 TB storage.

[https://degoo.com/](https://degoo.com/)

Unfortunately the apps (phone and web) are pitched to a very particular demography of user, and are rounded with great tools for storing photos, videos, music and documents in the cloud.

This makes it very difficult to use the cloud storage for flexible backup of data.

Here's a fairly impartial review you might find useful:

[Degoo Review 2020 - This Is Why You Shouldn't Use It](https://cloudstorageinfo.org/degoo-review)

Support seems hit and miss. They are a small company:

[https://degoo.com/about](https://degoo.com/about)

And have (only) two people on Customer Support so if they have [15 million users](https://www.techradar.com/news/the-best-cloud-storage#4-degoo), then  clearly they'd struggle to deliver customer support well. It's called outgrowing your boots.

They have also chopped and churned, originally using P2P storage then moving away form that after a load of [poor reviews]([Degoo Reviews | Read Customer Service Reviews of degoo.com](https://www.trustpilot.com/review/degoo.com)). They had a Windows desktop client, but no more. They are still, it seems clearly trying to find their niche in this market and establish a service model that secures a lasting future.

My interest is in keeping server data backed-up in the cloud.

And so, by studying their web app (which in written in Angular JS and managed with Webpack, communicating with a backend over a [graphQL]([GraphQL | A query language for your API](https://graphql.org/)) interface) I've written a simple CLI (command line interface) to the cloud storage.

It is written in Python and being developed under Linux. Being Python it's very likely highly portable, but there may be some small issues running on other systems. The only issues I can think of currently are:

* It tries to use os.sep intelligently to give you a natural feel if say you're using Windows where it's \ rather than Linux of MacOS where it's /. But that's untested so far on Windows.

* It uses [python-magic]([python-magic · PyPI](https://pypi.org/project/python-magic/)) to determine file types (needed for upload, as the API seems to demand this metadata). That may have some system dependencies.

A work in progress, it's not complete but at present it can reliably:

* log you in (if you provide valid credentials)

* list files and folders on the clour drive (ls, tree)

* navigate the cloud drive (cd, pwd)

* manipulate the cloud drive (mkdir, rm)

* download files from the cloud drive (get)

* upload files to the cloud drive (put)

Not implemented yet:

* [Top Secret Cloud Storage]([Top secret / zero knowledge storage : Degoo Help Center](https://help.degoo.com/support/solutions/articles/77000065516-top-secret-zero-knowledge-storage)

  * Degoo provide a good security focussed solution with their Top Secret vault, that they claim is 100% NSA proof. Only available on their phone app for now, not the web app. Will take some effort to analyse the cient-server interactions to provide CLI support.

* Device creation

  * Top level directories on your Degoo cloud drive are reserved for devices. Different licenses provide different numbers of devices. Currently you can delete a device but there is no facility for adding one again, or if you're an Ulitmate license holder adding new devices (which should be possible, but the web interface provides no such facility).
