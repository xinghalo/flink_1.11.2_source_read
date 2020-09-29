/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
/**
 * API class that public to users to handle the modal instance.
 * NzModalRef is aim to avoid accessing to the modal instance directly by users.
 * @abstract
 * @template T, R
 */
// tslint:disable-next-line:no-any
var /**
 * API class that public to users to handle the modal instance.
 * NzModalRef is aim to avoid accessing to the modal instance directly by users.
 * @abstract
 * @template T, R
 */
// tslint:disable-next-line:no-any
NzModalRef = /** @class */ (function () {
    function NzModalRef() {
    }
    return NzModalRef;
}());
/**
 * API class that public to users to handle the modal instance.
 * NzModalRef is aim to avoid accessing to the modal instance directly by users.
 * @abstract
 * @template T, R
 */
// tslint:disable-next-line:no-any
export { NzModalRef };
if (false) {
    /** @type {?} */
    NzModalRef.prototype.afterOpen;
    /** @type {?} */
    NzModalRef.prototype.afterClose;
    /**
     * @abstract
     * @return {?}
     */
    NzModalRef.prototype.open = function () { };
    /**
     * @abstract
     * @param {?=} result
     * @return {?}
     */
    NzModalRef.prototype.close = function (result) { };
    /**
     * @abstract
     * @param {?=} result
     * @return {?}
     */
    NzModalRef.prototype.destroy = function (result) { };
    /**
     * Trigger the nzOnOk/nzOnCancel by manual
     * @abstract
     * @return {?}
     */
    NzModalRef.prototype.triggerOk = function () { };
    /**
     * @abstract
     * @return {?}
     */
    NzModalRef.prototype.triggerCancel = function () { };
    /**
     * Return the component instance of nzContent when specify nzContent as a Component
     * Note: this method may return undefined if the Component has not ready yet. (it only available after Modal's ngOnInit)
     * @abstract
     * @return {?}
     */
    NzModalRef.prototype.getContentComponent = function () { };
    /**
     * Get the dom element of this Modal
     * @abstract
     * @return {?}
     */
    NzModalRef.prototype.getElement = function () { };
    /**
     * Get the instance of the Modal itself
     * @abstract
     * @return {?}
     */
    NzModalRef.prototype.getInstance = function () { };
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbW9kYWwtcmVmLmNsYXNzLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbIm1vZGFsL256LW1vZGFsLXJlZi5jbGFzcy50cyJdLCJuYW1lcyI6W10sIm1hcHBpbmdzIjoiOzs7Ozs7Ozs7OztBQVNBOzs7Ozs7OztJQUFBO0lBbUNBLENBQUM7SUFBRCxpQkFBQztBQUFELENBQUMsQUFuQ0QsSUFtQ0M7Ozs7Ozs7Ozs7O0lBbENDLCtCQUFxQzs7SUFDckMsZ0NBQW1DOzs7OztJQUVuQyw0Q0FBc0I7Ozs7OztJQUN0QixtREFBaUM7Ozs7OztJQUNqQyxxREFBbUM7Ozs7OztJQUtuQyxpREFBMkI7Ozs7O0lBQzNCLHFEQUErQjs7Ozs7OztJQVkvQiwyREFBa0M7Ozs7OztJQUtsQyxrREFBbUM7Ozs7OztJQUtuQyxtREFBeUMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBPYnNlcnZhYmxlIH0gZnJvbSAncnhqcyc7XG5cbmltcG9ydCB7IE56TW9kYWxDb21wb25lbnQgfSBmcm9tICcuL256LW1vZGFsLmNvbXBvbmVudCc7XG5cbi8qKlxuICogQVBJIGNsYXNzIHRoYXQgcHVibGljIHRvIHVzZXJzIHRvIGhhbmRsZSB0aGUgbW9kYWwgaW5zdGFuY2UuXG4gKiBOek1vZGFsUmVmIGlzIGFpbSB0byBhdm9pZCBhY2Nlc3NpbmcgdG8gdGhlIG1vZGFsIGluc3RhbmNlIGRpcmVjdGx5IGJ5IHVzZXJzLlxuICovXG4vLyB0c2xpbnQ6ZGlzYWJsZS1uZXh0LWxpbmU6bm8tYW55XG5leHBvcnQgYWJzdHJhY3QgY2xhc3MgTnpNb2RhbFJlZjxUID0gYW55LCBSID0gYW55PiB7XG4gIGFic3RyYWN0IGFmdGVyT3BlbjogT2JzZXJ2YWJsZTx2b2lkPjtcbiAgYWJzdHJhY3QgYWZ0ZXJDbG9zZTogT2JzZXJ2YWJsZTxSPjtcblxuICBhYnN0cmFjdCBvcGVuKCk6IHZvaWQ7XG4gIGFic3RyYWN0IGNsb3NlKHJlc3VsdD86IFIpOiB2b2lkO1xuICBhYnN0cmFjdCBkZXN0cm95KHJlc3VsdD86IFIpOiB2b2lkO1xuXG4gIC8qKlxuICAgKiBUcmlnZ2VyIHRoZSBuek9uT2svbnpPbkNhbmNlbCBieSBtYW51YWxcbiAgICovXG4gIGFic3RyYWN0IHRyaWdnZXJPaygpOiB2b2lkO1xuICBhYnN0cmFjdCB0cmlnZ2VyQ2FuY2VsKCk6IHZvaWQ7XG5cbiAgLy8gLyoqXG4gIC8vICAqIFJldHVybiB0aGUgQ29tcG9uZW50UmVmIG9mIG56Q29udGVudCB3aGVuIHNwZWNpZnkgbnpDb250ZW50IGFzIGEgQ29tcG9uZW50XG4gIC8vICAqIE5vdGU6IHRoaXMgbWV0aG9kIG1heSByZXR1cm4gdW5kZWZpbmVkIGlmIHRoZSBDb21wb25lbnQgaGFzIG5vdCByZWFkeSB5ZXQuIChpdCBvbmx5IGF2YWlsYWJsZSBhZnRlciBNb2RhbCdzIG5nT25Jbml0KVxuICAvLyAgKi9cbiAgLy8gYWJzdHJhY3QgZ2V0Q29udGVudENvbXBvbmVudFJlZigpOiBDb21wb25lbnRSZWY8e30+O1xuXG4gIC8qKlxuICAgKiBSZXR1cm4gdGhlIGNvbXBvbmVudCBpbnN0YW5jZSBvZiBuekNvbnRlbnQgd2hlbiBzcGVjaWZ5IG56Q29udGVudCBhcyBhIENvbXBvbmVudFxuICAgKiBOb3RlOiB0aGlzIG1ldGhvZCBtYXkgcmV0dXJuIHVuZGVmaW5lZCBpZiB0aGUgQ29tcG9uZW50IGhhcyBub3QgcmVhZHkgeWV0LiAoaXQgb25seSBhdmFpbGFibGUgYWZ0ZXIgTW9kYWwncyBuZ09uSW5pdClcbiAgICovXG4gIGFic3RyYWN0IGdldENvbnRlbnRDb21wb25lbnQoKTogVDtcblxuICAvKipcbiAgICogR2V0IHRoZSBkb20gZWxlbWVudCBvZiB0aGlzIE1vZGFsXG4gICAqL1xuICBhYnN0cmFjdCBnZXRFbGVtZW50KCk6IEhUTUxFbGVtZW50O1xuXG4gIC8qKlxuICAgKiBHZXQgdGhlIGluc3RhbmNlIG9mIHRoZSBNb2RhbCBpdHNlbGZcbiAgICovXG4gIGFic3RyYWN0IGdldEluc3RhbmNlKCk6IE56TW9kYWxDb21wb25lbnQ7XG59XG4iXX0=