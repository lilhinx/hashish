# Hashish

Hashish provides a simple implementation of a collection of hash tables for in-memory and on-disk storage of serialized model objects. It internally enforces sequential writes to data which generate published updates which can be observed via Combine subscriptions. 

Model objects conform to the SwiftProtobuf message protocol, and are assumed to be protobuf generated model classes.  

Model objects can optionally store a metadata object alongside the model object, which can be updated independently. 

An interface is provided to group mutatations of the collections into transactions to limit the publishing of updates.   

Model classes can opt-in to an optimistic locking scheme, where the model being put must exhibit an increment to a version propererty, or the update is ignored.
