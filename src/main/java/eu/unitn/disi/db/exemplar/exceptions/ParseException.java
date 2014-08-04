/*
 * Copyright (C) 2012 Davide Mottin <mottin@disi.unitn.eu>
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
package eu.unitn.disi.db.exemplar.exceptions;

/**
 * Exception thrown when an error occurs in the parsi phase
 * @author Davide Mottin <mottin@disi.unitn.eu>
 */
public class ParseException extends GenericException { 
    
    public ParseException(Throwable cause) {
        super(cause);
    }

    public ParseException(String message, Throwable cause, Object... params) {
        super(message, cause);
    }

    public ParseException(String message, Object... params) {
        super(message);
    }
}
