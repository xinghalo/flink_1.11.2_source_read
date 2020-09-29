/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Injectable } from '@angular/core';
import { merge, BehaviorSubject, Subject } from 'rxjs';
var NzMenuService = /** @class */ (function () {
    function NzMenuService() {
        this.menuItemClick$ = new Subject();
        this.theme$ = new Subject();
        this.mode$ = new BehaviorSubject('vertical');
        this.inlineIndent$ = new BehaviorSubject(24);
        this.check$ = merge(this.theme$, this.mode$, this.inlineIndent$);
        this.theme = 'light';
        this.mode = 'vertical';
        this.inlineIndent = 24;
        this.menuOpen$ = new BehaviorSubject(false);
    }
    /**
     * @param {?} menu
     * @return {?}
     */
    NzMenuService.prototype.onMenuItemClick = /**
     * @param {?} menu
     * @return {?}
     */
    function (menu) {
        this.menuItemClick$.next(menu);
    };
    /**
     * @param {?} mode
     * @return {?}
     */
    NzMenuService.prototype.setMode = /**
     * @param {?} mode
     * @return {?}
     */
    function (mode) {
        this.mode = mode;
        this.mode$.next(mode);
    };
    /**
     * @param {?} theme
     * @return {?}
     */
    NzMenuService.prototype.setTheme = /**
     * @param {?} theme
     * @return {?}
     */
    function (theme) {
        this.theme = theme;
        this.theme$.next(theme);
    };
    /**
     * @param {?} indent
     * @return {?}
     */
    NzMenuService.prototype.setInlineIndent = /**
     * @param {?} indent
     * @return {?}
     */
    function (indent) {
        this.inlineIndent = indent;
        this.inlineIndent$.next(indent);
    };
    NzMenuService.decorators = [
        { type: Injectable }
    ];
    return NzMenuService;
}());
export { NzMenuService };
if (false) {
    /** @type {?} */
    NzMenuService.prototype.isInDropDown;
    /** @type {?} */
    NzMenuService.prototype.menuItemClick$;
    /** @type {?} */
    NzMenuService.prototype.theme$;
    /** @type {?} */
    NzMenuService.prototype.mode$;
    /** @type {?} */
    NzMenuService.prototype.inlineIndent$;
    /** @type {?} */
    NzMenuService.prototype.check$;
    /** @type {?} */
    NzMenuService.prototype.theme;
    /** @type {?} */
    NzMenuService.prototype.mode;
    /** @type {?} */
    NzMenuService.prototype.inlineIndent;
    /** @type {?} */
    NzMenuService.prototype.menuOpen$;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoibnotbWVudS5zZXJ2aWNlLmpzIiwic291cmNlUm9vdCI6Im5nOi8vbmctem9ycm8tYW50ZC8iLCJzb3VyY2VzIjpbIm1lbnUvbnotbWVudS5zZXJ2aWNlLnRzIl0sIm5hbWVzIjpbXSwibWFwcGluZ3MiOiI7Ozs7QUFBQSxPQUFPLEVBQUUsVUFBVSxFQUFFLE1BQU0sZUFBZSxDQUFDO0FBQzNDLE9BQU8sRUFBRSxLQUFLLEVBQUUsZUFBZSxFQUFFLE9BQU8sRUFBRSxNQUFNLE1BQU0sQ0FBQztBQUl2RDtJQUFBO1FBR0UsbUJBQWMsR0FBRyxJQUFJLE9BQU8sRUFBdUIsQ0FBQztRQUNwRCxXQUFNLEdBQUcsSUFBSSxPQUFPLEVBQUUsQ0FBQztRQUN2QixVQUFLLEdBQUcsSUFBSSxlQUFlLENBQXFCLFVBQVUsQ0FBQyxDQUFDO1FBQzVELGtCQUFhLEdBQUcsSUFBSSxlQUFlLENBQVMsRUFBRSxDQUFDLENBQUM7UUFDaEQsV0FBTSxHQUFHLEtBQUssQ0FBQyxJQUFJLENBQUMsTUFBTSxFQUFFLElBQUksQ0FBQyxLQUFLLEVBQUUsSUFBSSxDQUFDLGFBQWEsQ0FBQyxDQUFDO1FBQzVELFVBQUssR0FBcUIsT0FBTyxDQUFDO1FBQ2xDLFNBQUksR0FBdUIsVUFBVSxDQUFDO1FBQ3RDLGlCQUFZLEdBQUcsRUFBRSxDQUFDO1FBQ2xCLGNBQVMsR0FBRyxJQUFJLGVBQWUsQ0FBVSxLQUFLLENBQUMsQ0FBQztJQW9CbEQsQ0FBQzs7Ozs7SUFsQkMsdUNBQWU7Ozs7SUFBZixVQUFnQixJQUF5QjtRQUN2QyxJQUFJLENBQUMsY0FBYyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztJQUNqQyxDQUFDOzs7OztJQUVELCtCQUFPOzs7O0lBQVAsVUFBUSxJQUF3QjtRQUM5QixJQUFJLENBQUMsSUFBSSxHQUFHLElBQUksQ0FBQztRQUNqQixJQUFJLENBQUMsS0FBSyxDQUFDLElBQUksQ0FBQyxJQUFJLENBQUMsQ0FBQztJQUN4QixDQUFDOzs7OztJQUVELGdDQUFROzs7O0lBQVIsVUFBUyxLQUF1QjtRQUM5QixJQUFJLENBQUMsS0FBSyxHQUFHLEtBQUssQ0FBQztRQUNuQixJQUFJLENBQUMsTUFBTSxDQUFDLElBQUksQ0FBQyxLQUFLLENBQUMsQ0FBQztJQUMxQixDQUFDOzs7OztJQUVELHVDQUFlOzs7O0lBQWYsVUFBZ0IsTUFBYztRQUM1QixJQUFJLENBQUMsWUFBWSxHQUFHLE1BQU0sQ0FBQztRQUMzQixJQUFJLENBQUMsYUFBYSxDQUFDLElBQUksQ0FBQyxNQUFNLENBQUMsQ0FBQztJQUNsQyxDQUFDOztnQkE5QkYsVUFBVTs7SUErQlgsb0JBQUM7Q0FBQSxBQS9CRCxJQStCQztTQTlCWSxhQUFhOzs7SUFDeEIscUNBQXNCOztJQUN0Qix1Q0FBb0Q7O0lBQ3BELCtCQUF1Qjs7SUFDdkIsOEJBQTREOztJQUM1RCxzQ0FBZ0Q7O0lBQ2hELCtCQUE0RDs7SUFDNUQsOEJBQWtDOztJQUNsQyw2QkFBc0M7O0lBQ3RDLHFDQUFrQjs7SUFDbEIsa0NBQWdEIiwic291cmNlc0NvbnRlbnQiOlsiaW1wb3J0IHsgSW5qZWN0YWJsZSB9IGZyb20gJ0Bhbmd1bGFyL2NvcmUnO1xuaW1wb3J0IHsgbWVyZ2UsIEJlaGF2aW9yU3ViamVjdCwgU3ViamVjdCB9IGZyb20gJ3J4anMnO1xuaW1wb3J0IHsgTnpEaXJlY3Rpb25WSElUeXBlIH0gZnJvbSAnLi4vY29yZS90eXBlcy9kaXJlY3Rpb24nO1xuaW1wb3J0IHsgTnpNZW51SXRlbURpcmVjdGl2ZSB9IGZyb20gJy4vbnotbWVudS1pdGVtLmRpcmVjdGl2ZSc7XG5cbkBJbmplY3RhYmxlKClcbmV4cG9ydCBjbGFzcyBOek1lbnVTZXJ2aWNlIHtcbiAgaXNJbkRyb3BEb3duOiBib29sZWFuO1xuICBtZW51SXRlbUNsaWNrJCA9IG5ldyBTdWJqZWN0PE56TWVudUl0ZW1EaXJlY3RpdmU+KCk7XG4gIHRoZW1lJCA9IG5ldyBTdWJqZWN0KCk7XG4gIG1vZGUkID0gbmV3IEJlaGF2aW9yU3ViamVjdDxOekRpcmVjdGlvblZISVR5cGU+KCd2ZXJ0aWNhbCcpO1xuICBpbmxpbmVJbmRlbnQkID0gbmV3IEJlaGF2aW9yU3ViamVjdDxudW1iZXI+KDI0KTtcbiAgY2hlY2skID0gbWVyZ2UodGhpcy50aGVtZSQsIHRoaXMubW9kZSQsIHRoaXMuaW5saW5lSW5kZW50JCk7XG4gIHRoZW1lOiAnbGlnaHQnIHwgJ2RhcmsnID0gJ2xpZ2h0JztcbiAgbW9kZTogTnpEaXJlY3Rpb25WSElUeXBlID0gJ3ZlcnRpY2FsJztcbiAgaW5saW5lSW5kZW50ID0gMjQ7XG4gIG1lbnVPcGVuJCA9IG5ldyBCZWhhdmlvclN1YmplY3Q8Ym9vbGVhbj4oZmFsc2UpO1xuXG4gIG9uTWVudUl0ZW1DbGljayhtZW51OiBOek1lbnVJdGVtRGlyZWN0aXZlKTogdm9pZCB7XG4gICAgdGhpcy5tZW51SXRlbUNsaWNrJC5uZXh0KG1lbnUpO1xuICB9XG5cbiAgc2V0TW9kZShtb2RlOiBOekRpcmVjdGlvblZISVR5cGUpOiB2b2lkIHtcbiAgICB0aGlzLm1vZGUgPSBtb2RlO1xuICAgIHRoaXMubW9kZSQubmV4dChtb2RlKTtcbiAgfVxuXG4gIHNldFRoZW1lKHRoZW1lOiAnbGlnaHQnIHwgJ2RhcmsnKTogdm9pZCB7XG4gICAgdGhpcy50aGVtZSA9IHRoZW1lO1xuICAgIHRoaXMudGhlbWUkLm5leHQodGhlbWUpO1xuICB9XG5cbiAgc2V0SW5saW5lSW5kZW50KGluZGVudDogbnVtYmVyKTogdm9pZCB7XG4gICAgdGhpcy5pbmxpbmVJbmRlbnQgPSBpbmRlbnQ7XG4gICAgdGhpcy5pbmxpbmVJbmRlbnQkLm5leHQoaW5kZW50KTtcbiAgfVxufVxuIl19