(defproject party-bus "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.cli "0.3.5"]
                 [medley "1.0.0"]
                 [manifold "0.1.6"]
                 [aleph "0.4.4"]
                 [gloss "0.2.6"]
                 [ring/ring-core "1.6.3"]
                 [ring/ring-defaults "0.3.1"]
                 [compojure "1.6.0"]]
  :main ^:skip-aot party-bus.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}
             :repl {:source-paths ["src" "dev"]
                    :main ^:skip-aot user
                    :clean-targets ^{:protect false} [:target-path]}})
