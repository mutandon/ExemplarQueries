/*
 * The MIT License
 *
 * Copyright 2017 Matteo Lissandrini <ml@disi.unitn.eu>.
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
package eu.unitn.disi.db.exemplar.utils.names;

import eu.unitn.disi.db.exemplar.utils.NamesProvider;
import eu.unitn.disi.db.mutilities.LoggableObject;

/**
 *
 * @author Matteo Lissandrini <ml@disi.unitn.eu>
 */
public class NoNames extends LoggableObject implements NamesProvider  {

   
    public NoNames() {
    
    }
    
    public NoNames(String namesDir) {
        info();
    }

    private void info (){
        debug("NO NAMES");
    }
    
    @Override
    public String getName(){
        return "NONE";
    }

    @Override
    public String getNodeNameFromID(Long id) {
        return  ""+id;
    }

    @Override
    public Long getNodeIDFromName(String name) {
        throw new UnsupportedOperationException("This class cannot convert names");
    }

    @Override
    public String getLabelNameFromID(Long id) {
        return ""+id;
    }

    @Override
    public Long getLabelIDFromName(String name) {
        throw new UnsupportedOperationException("This class cannot convert names");
    }

}
