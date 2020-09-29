/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { generate as generateColor } from 'ant-design-palettes';
/** @type {?} */
export var antIconConsolePrefix = '[@ant-design/icons-angular]: ';
/**
 * @param {?} message
 * @return {?}
 */
export function printErr(message) {
    console.error("" + antIconConsolePrefix + message + ".");
}
/**
 * @param {?} message
 * @return {?}
 */
export function printWarn(message) {
    console.warn("" + antIconConsolePrefix + message + ".");
}
/**
 * @param {?} primaryColor
 * @return {?}
 */
export function getSecondaryColor(primaryColor) {
    return generateColor(primaryColor)[0];
}
/**
 * @param {?} name
 * @param {?} theme
 * @return {?}
 */
export function withSuffix(name, theme) {
    switch (theme) {
        case 'fill': return name + "-fill";
        case 'outline': return name + "-o";
        case 'twotone': return name + "-twotone";
        case undefined: return name;
        default: throw new Error(antIconConsolePrefix + "Theme \"" + theme + "\" is not a recognized theme!");
    }
}
/**
 * @param {?} name
 * @param {?} theme
 * @param {?} pri
 * @param {?} sec
 * @return {?}
 */
export function withSuffixAndColor(name, theme, pri, sec) {
    return withSuffix(name, theme) + "-" + pri + "-" + sec;
}
/**
 * @param {?} abbr
 * @return {?}
 */
export function mapAbbrToTheme(abbr) {
    return abbr === 'o' ? 'outline' : (/** @type {?} */ (abbr));
}
/**
 * @param {?} name
 * @return {?}
 */
export function alreadyHasAThemeSuffix(name) {
    return name.endsWith('-fill') || name.endsWith('-o') || name.endsWith('-twotone');
}
/**
 * @param {?} target
 * @return {?}
 */
export function isIconDefinition(target) {
    return (typeof target === 'object' &&
        typeof target.name === 'string' &&
        (typeof target.theme === 'string' || target.theme === undefined) &&
        typeof target.icon === 'string');
}
/**
 * Get an `IconDefinition` object from abbreviation type, like `account-book-fill`.
 * @param {?} str
 * @return {?}
 */
export function getIconDefinitionFromAbbr(str) {
    /** @type {?} */
    var arr = str.split('-');
    /** @type {?} */
    var theme = mapAbbrToTheme(arr.splice(arr.length - 1, 1)[0]);
    /** @type {?} */
    var name = arr.join('-');
    return (/** @type {?} */ ({
        name: name,
        theme: theme,
        icon: ''
    }));
}
/**
 * @param {?} svg
 * @return {?}
 */
export function cloneSVG(svg) {
    return (/** @type {?} */ (svg.cloneNode(true)));
}
/**
 * Parse inline SVG string and replace colors with placeholders. For twotone icons only.
 * @param {?} raw
 * @return {?}
 */
export function replaceFillColor(raw) {
    return raw
        .replace(/['"]#333['"]/g, '"primaryColor"')
        .replace(/['"]#E6E6E6['"]/g, '"secondaryColor"')
        .replace(/['"]#D9D9D9['"]/g, '"secondaryColor"')
        .replace(/['"]#D8D8D8['"]/g, '"secondaryColor"');
}
/**
 * Split a name with namespace in it into a tuple like [ name, namespace ].
 * @param {?} type
 * @return {?}
 */
export function getNameAndNamespace(type) {
    /** @type {?} */
    var split = type.split(':');
    switch (split.length) {
        case 1: return [type, ''];
        case 2: return [split[1], split[0]];
        default: throw new Error(antIconConsolePrefix + "The icon type " + type + " is not valid!");
    }
}
/**
 * @param {?} type
 * @return {?}
 */
export function hasNamespace(type) {
    return getNameAndNamespace(type)[1] !== '';
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidXRpbHMuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9AYW50LWRlc2lnbi9pY29ucy1hbmd1bGFyLyIsInNvdXJjZXMiOlsidXRpbHMudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBRSxRQUFRLElBQUksYUFBYSxFQUFFLE1BQU0scUJBQXFCLENBQUM7O0FBR2hFLE1BQU0sS0FBTyxvQkFBb0IsR0FBRywrQkFBK0I7Ozs7O0FBRW5FLE1BQU0sVUFBVSxRQUFRLENBQUMsT0FBZTtJQUN0QyxPQUFPLENBQUMsS0FBSyxDQUFDLEtBQUcsb0JBQW9CLEdBQUcsT0FBTyxNQUFHLENBQUMsQ0FBQztBQUN0RCxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxTQUFTLENBQUMsT0FBZTtJQUN2QyxPQUFPLENBQUMsSUFBSSxDQUFDLEtBQUcsb0JBQW9CLEdBQUcsT0FBTyxNQUFHLENBQUMsQ0FBQztBQUNyRCxDQUFDOzs7OztBQUVELE1BQU0sVUFBVSxpQkFBaUIsQ0FBQyxZQUFvQjtJQUNwRCxPQUFPLGFBQWEsQ0FBQyxZQUFZLENBQUMsQ0FBQyxDQUFDLENBQUMsQ0FBQztBQUN4QyxDQUFDOzs7Ozs7QUFFRCxNQUFNLFVBQVUsVUFBVSxDQUFDLElBQVksRUFBRSxLQUE0QjtJQUNuRSxRQUFRLEtBQUssRUFBRTtRQUNiLEtBQUssTUFBTSxDQUFDLENBQUMsT0FBVSxJQUFJLFVBQU8sQ0FBQztRQUNuQyxLQUFLLFNBQVMsQ0FBQyxDQUFDLE9BQVUsSUFBSSxPQUFJLENBQUM7UUFDbkMsS0FBSyxTQUFTLENBQUMsQ0FBQyxPQUFVLElBQUksYUFBVSxDQUFDO1FBQ3pDLEtBQUssU0FBUyxDQUFDLENBQUMsT0FBTyxJQUFJLENBQUM7UUFDNUIsT0FBTyxDQUFDLENBQUMsTUFBTSxJQUFJLEtBQUssQ0FBSSxvQkFBb0IsZ0JBQVUsS0FBSyxrQ0FBOEIsQ0FBQyxDQUFDO0tBQ2hHO0FBQ0gsQ0FBQzs7Ozs7Ozs7QUFFRCxNQUFNLFVBQVUsa0JBQWtCLENB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cnQgeyBnZW5lcmF0ZSBhcyBnZW5lcmF0ZUNvbG9yIH0gZnJvbSAnYW50LWRlc2lnbi1wYWxldHRlcyc7XG5pbXBvcnQgeyBJY29uRGVmaW5pdGlvbiwgVGhlbWVUeXBlIH0gZnJvbSAnLi90eXBlcyc7XG5cbmV4cG9ydCBjb25zdCBhbnRJY29uQ29uc29sZVByZWZpeCA9ICdbQGFudC1kZXNpZ24vaWNvbnMtYW5ndWxhcl06ICc7XG5cbmV4cG9ydCBmdW5jdGlvbiBwcmludEVycihtZXNzYWdlOiBzdHJpbmcpOiB2b2lkIHtcbiAgY29uc29sZS5lcnJvcihgJHthbnRJY29uQ29uc29sZVByZWZpeH0ke21lc3NhZ2V9LmApO1xufVxuXG5leHBvcnQgZnVuY3Rpb24gcHJpbnRXYXJuKG1lc3NhZ2U6IHN0cmluZyk6IHZvaWQge1xuICBjb25zb2xlLndhcm4oYCR7YW50SWNvbkNvbnNvbGVQcmVmaXh9JHttZXNzYWdlfS5gKTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIGdldFNlY29uZGFyeUNvbG9yKHByaW1hcnlDb2xvcjogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIGdlbmVyYXRlQ29sb3IocHJpbWFyeUNvbG9yKVswXTtcbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHdpdGhTdWZmaXgobmFtZTogc3RyaW5nLCB0aGVtZTogVGhlbWVUeXBlIHwgdW5kZWZpbmVkKTogc3RyaW5nIHtcbiAgc3dpdGNoICh0aGVtZSkge1xuICAgIGNhc2UgJ2ZpbGwnOiByZXR1cm4gYCR7bmFtZX0tZmlsbGA7XG4gICAgY2FzZSAnb3V0bGluZSc6IHJldHVybiBgJHtuYW1lfS1vYDtcbiAgICBjYXNlICd0d290b25lJzogcmV0dXJuIGAke25hbWV9LXR3b3RvbmVgO1xuICAgIGNhc2UgdW5kZWZpbmVkOiByZXR1cm4gbmFtZTtcbiAgICBkZWZhdWx0OiB0aHJvdyBuZXcgRXJyb3IoYCR7YW50SWNvbkNvbnNvbGVQcmVmaXh9VGhlbWUgXCIke3RoZW1lfVwiIGlzIG5vdCBhIHJlY29nbml6ZWQgdGhlbWUhYCk7XG4gIH1cbn1cblxuZXhwb3J0IGZ1bmN0aW9uIHdpdGhTdWZmaXhBbmRDb2xvcihuYW1lOiBzdHJpbmcsIHRoZW1lOiBUaGVtZVR5cGUsIHByaTogc3RyaW5nLCBzZWM6IHN0cmluZyk6IHN0cmluZyB7XG4gIHJldHVybiBgJHt3aXRoU3VmZml4KG5hbWUsIHRoZW1lKX0tJHtwcml9LSR7c2VjfWA7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBtYXBBYmJyVG9UaGVtZShhYmJyOiBzdHJpbmcpOiBUaGVtZVR5cGUge1xuICByZXR1cm4gYWJiciA9PT0gJ28nID8gJ291dGxpbmUnIDogYWJiciBhcyBUaGVtZVR5cGU7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBhbHJlYWR5SGFzQVRoZW1lU3VmZml4KG5hbWU6IHN0cmluZyk6IGJvb2xlYW4ge1xuICByZXR1cm4gbmFtZS5lbmRzV2l0aCgnLWZpbGwnKSB8fCBuYW1lLmVuZHNXaXRoKCctbycpIHx8IG5hbWUuZW5kc1dpdGgoJy10d290b25lJyk7XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBpc0ljb25EZWZpbml0aW9uKHRhcmdldDogc3RyaW5nIHwgSWNvbkRlZmluaXRpb24pOiB0YXJnZXQgaXMgSWNvbkRlZmluaXRpb24ge1xuICByZXR1cm4gKFxuICAgIHR5cGVvZiB0YXJnZXQgPT09ICdvYmplY3QnICYmXG4gICAgdHlwZW9mIHRhcmdldC5uYW1lID09PSAnc3RyaW5nJyAmJlxuICAgICh0eXBlb2YgdGFyZ2V0LnRoZW1lID09PSAnc3RyaW5nJyB8fCB0YXJnZXQudGhlbWUgPT09IHVuZGVmaW5lZCkgJiZcbiAgICB0eXBlb2YgdGFyZ2V0Lmljb24gPT09ICdzdHJpbmcnXG4gICk7XG59XG5cbi8qKlxuICogR2V0IGFuIGBJY29uRGVmaW5pdGlvbmAgb2JqZWN0IGZyb20gYWJicmV2aWF0aW9uIHR5cGUsIGxpa2UgYGFjY291bnQtYm9vay1maWxsYC5cbiAqIEBwYXJhbSBzdHJcbiAqL1xuZXhwb3J0IGZ1bmN0aW9uIGdldEljb25EZWZpbml0aW9uRnJvbUFiYnIoc3RyOiBzdHJpbmcpOiBJY29uRGVmaW5pdGlvbiB7XG4gIGNvbnN0IGFyciA9IHN0ci5zcGxpdCgnLScpO1xuICBjb25zdCB0aGVtZSA9IG1hcEFiYnJUb1RoZW1lKGFyci5zcGxpY2UoYXJyLmxlbmd0aCAtIDEsIDEpWzBdKTtcbiAgY29uc3QgbmFtZSA9IGFyci5qb2luKCctJyk7XG5cbiAgcmV0dXJuIHtcbiAgICBuYW1lLFxuICAgIHRoZW1lLFxuICAgIGljb246ICcnXG4gIH0gYXMgSWNvbkRlZmluaXRpb247XG59XG5cbmV4cG9ydCBmdW5jdGlvbiBjbG9uZVNWRyhzdmc6IFNWR0VsZW1lbnQpOiBTVkdFbGVtZW50IHtcbiAgcmV0dXJuIHN2Zy5jbG9uZU5vZGUodHJ1ZSkgYXMgU1ZHRWxlbWVudDtcbn1cblxuLyoqXG4gKiBQYXJzZSBpbmxpbmUgU1ZHIHN0cmluZyBhbmQgcmVwbGFjZSBjb2xvcnMgd2l0aCBwbGFjZWhvbGRlcnMuIEZvciB0d290b25lIGljb25zIG9ubHkuXG4gKi9cbmV4cG9ydCBmdW5jdGlvbiByZXBsYWNlRmlsbENvbG9yKHJhdzogc3RyaW5nKTogc3RyaW5nIHtcbiAgcmV0dXJuIHJhd1xuICAgIC5yZXBsYWNlKC9bJ1wiXSMzMzNbJ1wiXS9nLCAnXCJwcmltYXJ5Q29sb3JcIicpXG4gICAgLnJlcGxhY2UoL1snXCJdI0U2RTZFNlsnXCJdL2csICdcInNlY29uZGFyeUNvbG9yXCInKVxuICAgIC5yZXBsYWNlKC9bJ1wiXSNEOUQ5RDlbJ1wiXS9nLCAnXCJzZWNvbmRhcnlDb2xvclwiJylcbiAgICAucmVwbGFjZSgvWydcIl0jRDhEOEQ4WydcIl0vZywgJ1wic2Vjb25kYXJ5Q29sb3JcIicpO1xufVxuXG4vKipcbiAqIFNwbGl0IGEgbmFtZSB3aXRoIG5hbWVzcGFjZSBpbiBpdCBpbnRvIGEgdHVwbGUgbGlrZSBbIG5hbWUsIG5hbWVzcGFjZSBdLlxuICovXG5leHBvcnQgZnVuY3Rpb24gZ2V0TmFtZUFuZE5hbWVzcGFjZSh0eXBlOiBzdHJpbmcpOiBbc3RyaW5nLCBzdHJpbmddIHtcbiAgY29uc3Qgc3BsaXQgPSB0eXBlLnNwbGl0KCc6Jyk7XG4gIHN3aXRjaCAoc3BsaXQubGVuZ3RoKSB7XG4gICAgY2FzZSAxOiByZXR1cm4gW3R5cGUsICcnXTtcbiAgICBjYXNlIDI6IHJldHVybiBbc3BsaXRbMV0sIHNwbGl0WzBdXTtcbiAgICBkZWZhdWx0OiB0aHJvdyBuZXcgRXJyb3IoYCR7YW50SWNvbkNvbnNvbGVQcmVmaXh9VGhlIGljb24gdHlwZSAke3R5cGV9IGlzIG5vdCB2YWxpZCFgKTtcbiAgfVxufVxuXG5leHBvcnQgZnVuY3Rpb24gaGFzTmFtZXNwYWNlKHR5cGU6IHN0cmluZyk6IGJvb2xlYW4ge1xuICByZXR1cm4gZ2V0TmFtZUFuZE5hbWVzcGFjZSh0eXBlKVsxXSAhPT0gJyc7XG59XG4iXX0=