#include <stdio.h>
#include <string.h>
#include <syscall.h>

int main (int argc, char *argv[]) {
  int i;

  if (argc <= 1) {
    return EXIT_SUCCESS;
  }

  for (i = 1; i < argc; i++) {
    char *str = argv[i];
    int len = strlen (str);

    if (len >= 2 && ((str[0] == '\'' && str[len - 1] == '\'') || 
                     (str[0] == '"' && str[len - 1] == '"'))) {
      str[len - 1] = '\0';
      str++;
    }

    printf ("%s ", str);
  }
  printf ("\n");

  return EXIT_SUCCESS;
}