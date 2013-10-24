jessy
=====
  A large family of distributed transactional protocols have a common structure, called Deferred Update Replication (DUR).
  Protocols of this family differ only in the specific behavior of a few generic functions.
  Based on this insight, we offer a generic DUR framework, called jessy, along with a library of finely-optimized plug-in implementations of the
  required behaviors.
  By mixing and matching the appropriate plug-ins, the framework can be customized to provide a high-performance implementation of diverse
  transactional protocols, which in turn enables a fair, apples-to-apples comparison between them.
  
  we also have tailored jessy, and implemented five prominent transactional protocols published in the past few years (Peluso2012,Sciascia2012,Sovran2011,Schiper2010,Serrano2007).
