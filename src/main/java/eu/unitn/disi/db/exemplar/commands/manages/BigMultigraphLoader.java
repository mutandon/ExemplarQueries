/*
 * Copyright (C) 2013 Davide Mottin <mottin@disi.unitn.eu>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
 */

package eu.unitn.disi.db.exemplar.commands.manages;

import eu.unitn.disi.db.command.CommandInput;
import eu.unitn.disi.db.command.exceptions.ExecutionException;
import eu.unitn.disi.db.command.global.LoaderCommand;
import eu.unitn.disi.db.grava.graphs.BigMultigraph;
import eu.unitn.disi.db.mutilities.StringUtils;
import eu.unitn.disi.db.mutilities.exceptions.ParseException;
import java.io.IOException;



/**
 *
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class BigMultigraphLoader extends LoaderCommand {
    private String graphPath;

    @Override
    protected void execute() throws ExecutionException {
        try {
            loadedObject = new BigMultigraph(graphPath + "-sin.graph", graphPath + "-sout.graph", StringUtils.countLines((graphPath + "-sin.graph")));
        } catch (ParseException | IOException ex) {
            throw new ExecutionException(ex);
        }
    }

    @CommandInput(
        consoleFormat = "-kb",
        defaultValue = "",
        description = "path to the knowledgbase sin and sout files, just up to the prefix, like InputData/freebase ",
        mandatory = true)
    public void setGraphPath(String graphPath) {
        this.graphPath = graphPath;
    }

    @Override
    protected String commandDescription() {
        return "Load a big multigraph into main memory";
    }

}
