(ns mesos-example.protobuf
  (:require
   [clojure.reflect :as reflect]
   [clojure.string :as string]
   [clojure.pprint :as pprint])
  (:import org.apache.mesos.Protos))


;;; Message Fields

;;; Reflect on a Java Protobuf GeneratedMessage class to find its
;;; fields. Use this as a basis for generating clojure-compatibility
;;; methods.

(def ^{:private true :doc "Ignore these field names" }
  IGNORE-FIELDS
  #{'bitField0_})

(defn- endswith-underscore?
  [some-name]
  (if-let [string-name (and some-name (name some-name))]
    (= \_ (last string-name))))

(defn- without-underscore
  [some-name]
  (if-let [string-name (and some-name (name some-name))]
    (if (endswith-underscore? string-name)
      (subs string-name 0 (dec (.length string-name)))
      string-name)))

(defn- clojure-field-name
  [proto-name]
  (symbol
   (string/replace
    (without-underscore proto-name)
    #"[A-Z]"
    (fn [$0] (str "-" (string/lower-case $0))))))

(defn- camel-case
  [string-name]
  (string/replace
   string-name
   #"-(\w)"
   (fn [[$0 $1]]
     (.toUpperCase $1))))

(defn- java-partial
  [proto-name]
  (if-let [string-name (without-underscore proto-name)]
    (str (.toUpperCase (subs string-name 0 1))
         (camel-case (subs string-name 1)))))

(defn- field-class
  [type]
  (cond
   (isa? type java.util.Collection) :list
   (isa? type com.google.protobuf.GeneratedMessage) :protobuf
   true :value))

(defn- classify-field
  "Given a member reflected from a Java class, determine if it's a
  protocol buffer field. If it is, determine its name and classify
  it."
  [member]
  (let [{name :name type-name :type} member]
    (when (and type-name (nil? (IGNORE-FIELDS name)) (endswith-underscore? name))
      (let [type (eval type-name)]
        {:name (clojure-field-name name)
         :java-name (java-partial name)
         :class (field-class type) }))))

(defn- message-fields
  "Reflect on a Java class representing a protocol buffer message to
  determine its fields."
  [message]
  (filter identity (map classify-field (:members (reflect/reflect message)))))


;;; ProtoBuf Protocol

(defprotocol ProtoBuf
  (parse [message-object]))


;;; Macro Helpers

(defn- predicate-name
  [constructor-name]
  (symbol (str (name constructor-name) "?")))

(defn- builder-name
  [message-name]
  (symbol (str (name message-name) "/newBuilder")))

(defn- getter-name
  [partial repeated]
  (symbol
   (if repeated
     (str ".get" partial "List")
     (str ".get" partial))))

(defn- setter-name
  [partial repeated]
  (symbol
   (if repeated
     (str ".add" partial)
     (str ".set" partial))))

(defn- coerce-name
  [protocol]
  (symbol (str 'map-> (name protocol))))

(defn- field-definitions
  [definition]
  (let [[full-name protocol] definition
        field-name (name full-name)
        partial (java-partial field-name)
        repeated (vector? protocol)
        protocol (if repeated (first protocol) protocol)]
    {:name field-name
     :key (keyword field-name)
     :partial partial
     :getter (getter-name partial repeated)
     :setter (setter-name partial repeated)
     :protocol protocol
     :repeated repeated
     :coerce (and protocol (coerce-name protocol))}))


;;; defprotobox

(defn emit-field-getter
  [obj field]
  (let [get-expr `(~(:getter field) ~obj)
        protocol (:protocol field)
        repeated (:repeated field)]

    (cond
     (and repeated protocol)
     `(map parse ~get-expr)

     (and protocol (not repeated))
     `(parse ~get-expr)

     :else
     get-expr)))

(defn emit-field-setter
  [builder field get-expr]
  (let [protocol (:protocol field)]
    (cond
     (:repeated field)
     (let [valsym (gensym 'val)
           val-expr (if protocol `(~(:coerce field) ~valsym) valsym)]
       `(doseq [~valsym ~get-expr]
          (~(:setter field) ~builder ~val-expr)))

     protocol
     `(~(:setter field) ~builder (~(:coerce field) ~get-expr))

     :else
     `(~(:setter field) ~builder ~get-expr))))

(defmacro defprotobox
  "A 'box' is a special protocol message that contains a single
  field. It's convenient to have the field's value boxed and unboxed
  automatically."

  [constructor-name message field-def]
  (let [predicate (predicate-name constructor-name)
        builder (builder-name message)
        field (field-definitions field-def)

        value-sym (gensym 'value)
        builder-sym (gensym 'builder)
        obj-sym (gensym 'obj)]

    `(let []
       (def ~predicate
         (fn [obj#]
           (instance? ~message obj#)))

       (def ~constructor-name
         (fn [~value-sym]
           (if (~predicate ~value-sym)
             ~value-sym
             (let [~builder-sym (~builder)]
               ~(emit-field-setter builder-sym field value-sym)
               (.build ~builder-sym)))))

       (def ~(symbol (coerce-name constructor-name)) ~constructor-name)

       (extend ~message
         ProtoBuf
         {:parse
          (fn [~obj-sym]
            ~(emit-field-getter obj-sym field)) })
       )))


;;; defproto

(defmacro defproto
  [constructor-name message & field-defs]
  (let [predicate (predicate-name constructor-name)
        builder (builder-name message)
        fields (map field-definitions field-defs)

        coerce (coerce-name constructor-name)
        ctor-value (gensym 'value)
        ctor-builder (gensym 'builder)
        parse-obj (gensym 'obj)]

    `(let []
       (def ~predicate
         (fn [obj#]
           (instance? ~message obj#)))

       (def ~coerce
         (fn [~ctor-value]
           (if (~predicate ~ctor-value)
             ~ctor-value
             (let [~ctor-builder (~builder)]
               ~@(map
                  (fn [field]
                    `(when (contains? ~ctor-value ~(:key field))
                       ~(emit-field-setter
                         ctor-builder
                         field
                         `(get ~ctor-value ~(:key field)))))
                  fields)
               (.build ~ctor-builder)))))

       (defn ~constructor-name
         [& kv-pairs#]
         (~coerce (apply hash-map kv-pairs#)))

       (extend ~message
         ProtoBuf
         {:parse
          (fn [~parse-obj]
            (hash-map
             ~@(mapcat
                (fn [field]
                  [(:key field) (emit-field-getter parse-obj field)])
                fields)))}
       ))))
