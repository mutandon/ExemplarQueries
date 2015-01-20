/*
 * The MIT License
 *
 * Copyright 2015 Matteo Lissandrini <ml@disi.unitn.eu>.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.unitn.disi.db.exemplar.core.utils;

import eu.unitn.disi.db.exemplar.core.RelatedQuery;
import eu.unitn.disi.db.grava.graphs.Multigraph;
import java.util.Collection;
import java.util.Map;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
class JsonRelated {

        private int key;
        private Map<String, String> query;
        private Multigraph graph;
        private Map<Long, String> names;
        private Collection<RelatedQuery> top_embedded;
        private Collection<RelatedQuery> top_external;
        private Collection<RelatedQuery> bottom;
        private Collection<RelatedQuery> random;
        private Collection<String> google;
        private Collection<String> bing;

        public JsonRelated() {
        }

        public int getKey() {
            return key;
        }

        public void setKey(int key) {
            this.key = key;
        }

        public Map<String, String> getQuery() {
            return query;
        }

        public void setQuery(Map<String, String> query) {
            this.query = query;
        }

        public Multigraph getGraph() {
            return graph;
        }

        public void setGraph(Multigraph graph) {
            this.graph = graph;
        }

        public Collection<String> getGoogle() {
            return google;
        }

        public void setGoogle(Collection<String> google) {
            this.google = google;
        }

        public Collection<String> getBing() {
            return bing;
        }

        public void setBing(Collection<String> bing) {
            this.bing = bing;
        }

        public Map<Long, String> getNames() {
            return names;
        }

        public void setNames(Map<Long, String> names) {
            this.names = names;
        }

        public Collection<RelatedQuery> getTop_embedded() {
            return top_embedded;
        }

        public void setTop_embedded(Collection<RelatedQuery> top_embedded) {
            this.top_embedded = top_embedded;
        }

        public Collection<RelatedQuery> getTop_external() {
            return top_external;
        }

        public void setTop_external(Collection<RelatedQuery> top_external) {
            this.top_external = top_external;
        }

        public Collection<RelatedQuery> getBottom() {
            return bottom;
        }

        public void setBottom(Collection<RelatedQuery> bottom) {
            this.bottom = bottom;
        }

        public Collection<RelatedQuery> getRandom() {
            return random;
        }

        public void setRandom(Collection<RelatedQuery> random) {
            this.random = random;
        }
    }
