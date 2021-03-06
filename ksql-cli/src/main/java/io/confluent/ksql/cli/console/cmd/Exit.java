/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.cli.console.cmd;

import io.confluent.ksql.cli.console.Console;
import java.util.Objects;
import org.jline.reader.EndOfFileException;

class Exit implements CliSpecificCommand {

  private final Console console;

  Exit(final Console console) {
    this.console = Objects.requireNonNull(console, "console");
  }

  @Override
  public String getName() {
    return "exit";
  }

  @Override
  public void printHelp() {
    console.writer().println("exit:");
    console.writer().println("\tExit the CLI.");
  }

  @Override
  public void execute(final String commandStrippedLine) {
    throw new EndOfFileException();
  }
}
