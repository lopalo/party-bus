(defproject party-bus "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :min-lein-version "2.7.1"
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/clojurescript "1.9.946"]
                 [org.clojure/core.async  "0.3.443"]
                 [org.clojure/tools.cli "0.3.5"]
                 [org.clojure/core.incubator "0.1.4"]
                 [medley "1.0.0"]
                 [manifold "0.1.6"]
                 [aleph "0.4.4"]
                 [gloss "0.2.6"]
                 [ring/ring-core "1.6.3"]
                 [ring/ring-defaults "0.3.1"]
                 [ring-cors "0.1.11"]
                 [compojure "1.6.0"]

                 [rum "0.11.2"]
                 [cljsjs/antd "3.2.0-0"
                  :exclusions [cljsjs/react cljsjs/react-dom]]
                 [antizer "0.2.2"
                  :exclusions [cljsjs/react cljsjs/react-dom cljsjs/antd]]
                 [cljs-http "0.1.44"]
                 [jarohen/chord "0.8.1"]
                 [digest "1.4.6"]
                 [cljs-hash "0.0.2"]]
  :source-paths ["src"]
  :main ^:skip-aot party-bus.main
  :target-path "target/%s"

  :plugins [[lein-figwheel "0.5.14"]
            [lein-cljsbuild "1.1.7"
             :exclusions [[org.clojure/clojure]]]]
  :cljsbuild {:builds
              [{:id "dev"
                :source-paths ["src"]
                :figwheel {:on-jsload "party-bus.simulator.ui.app/on-js-reload"}
                           ;:open-urls ["http://localhost:3449/index.html"]}
                :compiler {:main party-bus.simulator.ui.app
                           :asset-path "js/compiled/out"
                           :output-to "resources/public/js/compiled/party_bus.js"
                           :output-dir "resources/public/js/compiled/out"
                           :closure-defines
                           {party-bus.simulator.ui.core/INITIAL-ADDRESS
                            "127.0.0.1:12080"}
                           :parallel-build true
                           :source-map-timestamp true
                           :preloads [devtools.preload]}}
               {:id "min"
                :source-paths ["src"]
                :compiler {:output-to "resources/public/js/compiled/party_bus.js"
                           :main party-bus.simulator.ui.app
                           :optimizations :advanced
                           :parallel-build true
                           :pretty-print false}}]}
  :clean-targets ^{:protect false} ["resources/public/js/compiled"
                                    "resources/public/css/compiled"
                                    :target-path]
  :figwheel {:css-dirs ["resources/public/css"]
             :ring-handler party-bus.simulator.server/cljsjs-handler}
  :cljfmt {:indents {flow [[:block 0]]}}
  :profiles {:uberjar {:aot :all}
             :repl {:dependencies [[binaryage/devtools "0.9.4"]
                                   [figwheel-sidecar "0.5.14"]
                                   [com.cemerick/piggieback "0.2.2"]]
                    :global-vars {*warn-on-reflection* true}
                    :source-paths ["src" "dev"]
                    :main ^:skip-aot user
                    :repl-options {:nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}}})
