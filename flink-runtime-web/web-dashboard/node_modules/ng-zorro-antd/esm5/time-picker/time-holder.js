/**
 * @fileoverview added by tsickle
 * @suppress {checkTypes,extraRequire,missingOverride,missingReturn,unusedPrivateMembers,uselessCode} checked by tsc
 */
import { Subject } from 'rxjs';
import { isNotNil } from '../core/util/check';
var TimeHolder = /** @class */ (function () {
    function TimeHolder() {
        this._seconds = undefined;
        this._hours = undefined;
        this._minutes = undefined;
        this._defaultOpenValue = new Date();
        this._changes = new Subject();
    }
    /**
     * @return {?}
     */
    TimeHolder.prototype.setDefaultValueIfNil = /**
     * @return {?}
     */
    function () {
        if (!isNotNil(this._value)) {
            this._value = new Date(this.defaultOpenValue);
        }
    };
    /**
     * @template THIS
     * @this {THIS}
     * @param {?} value
     * @param {?} disabled
     * @return {THIS}
     */
    TimeHolder.prototype.setMinutes = /**
     * @template THIS
     * @this {THIS}
     * @param {?} value
     * @param {?} disabled
     * @return {THIS}
     */
    function (value, disabled) {
        if (disabled) {
            return (/** @type {?} */ (this));
        }
        (/** @type {?} */ (this)).setDefaultValueIfNil();
        (/** @type {?} */ (this)).minutes = value;
        return (/** @type {?} */ (this));
    };
    /**
     * @template THIS
     * @this {THIS}
     * @param {?} value
     * @param {?} disabled
     * @return {THIS}
     */
    TimeHolder.prototype.setHours = /**
     * @template THIS
     * @this {THIS}
     * @param {?} value
     * @param {?} disabled
     * @return {THIS}
     */
    function (value, disabled) {
        if (disabled) {
            return (/** @type {?} */ (this));
        }
        (/** @type {?} */ (this)).setDefaultValueIfNil();
        (/** @type {?} */ (this)).hours = value;
        return (/** @type {?} */ (this));
    };
    /**
     * @template THIS
     * @this {THIS}
     * @param {?} value
     * @param {?} disabled
     * @return {THIS}
     */
    TimeHolder.prototype.setSeconds = /**
     * @template THIS
     * @this {THIS}
     * @param {?} value
     * @param {?} disabled
     * @return {THIS}
     */
    function (value, disabled) {
        if (disabled) {
            return (/** @type {?} */ (this));
        }
        (/** @type {?} */ (this)).setDefaultValueIfNil();
        (/** @type {?} */ (this)).seconds = value;
        return (/** @type {?} */ (this));
    };
    Object.defineProperty(TimeHolder.prototype, "changes", {
        get: /**
         * @return {?}
         */
        function () {
            return this._changes.asObservable();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(TimeHolder.prototype, "value", {
        get: /**
         * @return {?}
         */
        function () {
            return this._value;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (value !== this._value) {
                this._value = value;
                if (isNotNil(this._value)) {
                    this._hours = (/** @type {?} */ (this._value)).getHours();
                    this._minutes = (/** @type {?} */ (this._value)).getMinutes();
                    this._seconds = (/** @type {?} */ (this._value)).getSeconds();
                }
                else {
                    this._clear();
                }
            }
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @template THIS
     * @this {THIS}
     * @param {?} value
     * @return {THIS}
     */
    TimeHolder.prototype.setValue = /**
     * @template THIS
     * @this {THIS}
     * @param {?} value
     * @return {THIS}
     */
    function (value) {
        (/** @type {?} */ (this)).value = value;
        return (/** @type {?} */ (this));
    };
    /**
     * @return {?}
     */
    TimeHolder.prototype.clear = /**
     * @return {?}
     */
    function () {
        this._clear();
        this.update();
    };
    Object.defineProperty(TimeHolder.prototype, "isEmpty", {
        get: /**
         * @return {?}
         */
        function () {
            return !(isNotNil(this._hours) || isNotNil(this._minutes) || isNotNil(this._seconds));
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @private
     * @return {?}
     */
    TimeHolder.prototype._clear = /**
     * @private
     * @return {?}
     */
    function () {
        this._hours = undefined;
        this._minutes = undefined;
        this._seconds = undefined;
    };
    /**
     * @private
     * @return {?}
     */
    TimeHolder.prototype.update = /**
     * @private
     * @return {?}
     */
    function () {
        if (this.isEmpty) {
            this._value = undefined;
        }
        else {
            if (!isNotNil(this._hours)) {
                this._hours = this.defaultHours;
            }
            else {
                (/** @type {?} */ (this._value)).setHours((/** @type {?} */ (this.hours)));
            }
            if (!isNotNil(this._minutes)) {
                this._minutes = this.defaultMinutes;
            }
            else {
                (/** @type {?} */ (this._value)).setMinutes((/** @type {?} */ (this.minutes)));
            }
            if (!isNotNil(this._seconds)) {
                this._seconds = this.defaultSeconds;
            }
            else {
                (/** @type {?} */ (this._value)).setSeconds((/** @type {?} */ (this.seconds)));
            }
            this._value = new Date((/** @type {?} */ (this._value)));
        }
        this.changed();
    };
    /**
     * @return {?}
     */
    TimeHolder.prototype.changed = /**
     * @return {?}
     */
    function () {
        this._changes.next(this._value);
    };
    Object.defineProperty(TimeHolder.prototype, "hours", {
        get: /**
         * @return {?}
         */
        function () {
            return this._hours;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (value !== this._hours) {
                this._hours = value;
                this.update();
            }
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(TimeHolder.prototype, "minutes", {
        get: /**
         * @return {?}
         */
        function () {
            return this._minutes;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (value !== this._minutes) {
                this._minutes = value;
                this.update();
            }
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(TimeHolder.prototype, "seconds", {
        get: /**
         * @return {?}
         */
        function () {
            return this._seconds;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (value !== this._seconds) {
                this._seconds = value;
                this.update();
            }
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(TimeHolder.prototype, "defaultOpenValue", {
        get: /**
         * @return {?}
         */
        function () {
            return this._defaultOpenValue;
        },
        set: /**
         * @param {?} value
         * @return {?}
         */
        function (value) {
            if (this._defaultOpenValue !== value) {
                this._defaultOpenValue = value;
                this.update();
            }
        },
        enumerable: true,
        configurable: true
    });
    /**
     * @template THIS
     * @this {THIS}
     * @param {?} value
     * @return {THIS}
     */
    TimeHolder.prototype.setDefaultOpenValue = /**
     * @template THIS
     * @this {THIS}
     * @param {?} value
     * @return {THIS}
     */
    function (value) {
        (/** @type {?} */ (this)).defaultOpenValue = value;
        return (/** @type {?} */ (this));
    };
    Object.defineProperty(TimeHolder.prototype, "defaultHours", {
        get: /**
         * @return {?}
         */
        function () {
            return this._defaultOpenValue.getHours();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(TimeHolder.prototype, "defaultMinutes", {
        get: /**
         * @return {?}
         */
        function () {
            return this._defaultOpenValue.getMinutes();
        },
        enumerable: true,
        configurable: true
    });
    Object.defineProperty(TimeHolder.prototype, "defaultSeconds", {
        get: /**
         * @return {?}
         */
        function () {
            return this._defaultOpenValue.getSeconds();
        },
        enumerable: true,
        configurable: true
    });
    return TimeHolder;
}());
export { TimeHolder };
if (false) {
    /**
     * @type {?}
     * @private
     */
    TimeHolder.prototype._seconds;
    /**
     * @type {?}
     * @private
     */
    TimeHolder.prototype._hours;
    /**
     * @type {?}
     * @private
     */
    TimeHolder.prototype._minutes;
    /**
     * @type {?}
     * @private
     */
    TimeHolder.prototype._defaultOpenValue;
    /**
     * @type {?}
     * @private
     */
    TimeHolder.prototype._value;
    /**
     * @type {?}
     * @private
     */
    TimeHolder.prototype._changes;
}
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoidGltZS1ob2xkZXIuanMiLCJzb3VyY2VSb290Ijoibmc6Ly9uZy16b3Jyby1hbnRkLyIsInNvdXJjZXMiOlsidGltZS1waWNrZXIvdGltZS1ob2xkZXIudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7OztBQUFBLE9BQU8sRUFBYyxPQUFPLEVBQUUsTUFBTSxNQUFNLENBQUM7QUFFM0MsT0FBTyxFQUFFLFFBQVEsRUFBRSxNQUFNLG9CQUFvQixDQUFDO0FBRTlDO0lBOEtFO1FBN0tRLGFBQVEsR0FBdUIsU0FBUyxDQUFDO1FBQ3pDLFdBQU0sR0FBdUIsU0FBUyxDQUFDO1FBQ3ZDLGFBQVEsR0FBdUIsU0FBUyxDQUFDO1FBQ3pDLHNCQUFpQixHQUFTLElBQUksSUFBSSxFQUFFLENBQUM7UUFFckMsYUFBUSxHQUFHLElBQUksT0FBTyxFQUFRLENBQUM7SUF3S3hCLENBQUM7Ozs7SUF0S2hCLHlDQUFvQjs7O0lBQXBCO1FBQ0UsSUFBSSxDQUFDLFFBQVEsQ0FBQyxJQUFJLENBQUMsTUFBTSxDQUFDLEVBQUU7WUFDMUIsSUFBSSxDQUFDLE1BQU0sR0FBRyxJQUFJLElBQUksQ0FBQyxJQUFJLENBQUMsZ0JBQWdCLENBQUMsQ0FBQztTQUMvQztJQUNILENBQUM7Ozs7Ozs7O0lBRUQsK0JBQVU7Ozs7Ozs7SUFBVixVQUFXLEtBQWEsRUFBRSxRQUFpQjtRQUN6QyxJQUFJLFFBQVEsRUFBRTtZQUNaLE9BQU8sbUJBQUEsSUFBSSxFQUFBLENBQUM7U0FDYjtRQUNELG1CQUFBLElBQUksRUFBQSxDQUFDLG9CQUFvQixFQUFFLENBQUM7UUFDNUIsbUJBQUEsSUFBSSxFQUFBLENBQUMsT0FBTyxHQUFHLEtBQUssQ0FBQztRQUNyQixPQUFPLG1CQUFBLElBQUksRUFBQSxDQUFDO0lBQ2QsQ0FBQzs7Ozs7Ozs7SUFFRCw2QkFBUTs7Ozs7OztJQUFSLFVBQVMsS0FBYSxFQUFFLFFBQWlCO1FBQ3ZDLElBQUksUUFBUSxFQUFFO1lBQ1osT0FBTyxtQkFBQSxJQUFJLEVBQUEsQ0FB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QUE7SUFFRCxzQkFBSSxzQ0FBYzs7OztRQUFsQjtZQUNFLE9BQU8sSUFBSSxDQUFDLGlCQUFpQixDQUFDLFVBQVUsRUFBRSxDQUFDO1FBQzdDLENBQUM7OztPQUFBO0lBR0gsaUJBQUM7QUFBRCxDQUFDLEFBL0tELElBK0tDOzs7Ozs7O0lBOUtDLDhCQUFpRDs7Ozs7SUFDakQsNEJBQStDOzs7OztJQUMvQyw4QkFBaUQ7Ozs7O0lBQ2pELHVDQUE2Qzs7Ozs7SUFDN0MsNEJBQWlDOzs7OztJQUNqQyw4QkFBdUMiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBPYnNlcnZhYmxlLCBTdWJqZWN0IH0gZnJvbSAncnhqcyc7XG5cbmltcG9ydCB7IGlzTm90TmlsIH0gZnJvbSAnLi4vY29yZS91dGlsL2NoZWNrJztcblxuZXhwb3J0IGNsYXNzIFRpbWVIb2xkZXIge1xuICBwcml2YXRlIF9zZWNvbmRzOiBudW1iZXIgfCB1bmRlZmluZWQgPSB1bmRlZmluZWQ7XG4gIHByaXZhdGUgX2hvdXJzOiBudW1iZXIgfCB1bmRlZmluZWQgPSB1bmRlZmluZWQ7XG4gIHByaXZhdGUgX21pbnV0ZXM6IG51bWJlciB8IHVuZGVmaW5lZCA9IHVuZGVmaW5lZDtcbiAgcHJpdmF0ZSBfZGVmYXVsdE9wZW5WYWx1ZTogRGF0ZSA9IG5ldyBEYXRlKCk7XG4gIHByaXZhdGUgX3ZhbHVlOiBEYXRlIHwgdW5kZWZpbmVkO1xuICBwcml2YXRlIF9jaGFuZ2VzID0gbmV3IFN1YmplY3Q8RGF0ZT4oKTtcblxuICBzZXREZWZhdWx0VmFsdWVJZk5pbCgpOiB2b2lkIHtcbiAgICBpZiAoIWlzTm90TmlsKHRoaXMuX3ZhbHVlKSkge1xuICAgICAgdGhpcy5fdmFsdWUgPSBuZXcgRGF0ZSh0aGlzLmRlZmF1bHRPcGVuVmFsdWUpO1xuICAgIH1cbiAgfVxuXG4gIHNldE1pbnV0ZXModmFsdWU6IG51bWJlciwgZGlzYWJsZWQ6IGJvb2xlYW4pOiB0aGlzIHtcbiAgICBpZiAoZGlzYWJsZWQpIHtcbiAgICAgIHJldHVybiB0aGlzO1xuICAgIH1cbiAgICB0aGlzLnNldERlZmF1bHRWYWx1ZUlmTmlsKCk7XG4gICAgdGhpcy5taW51dGVzID0gdmFsdWU7XG4gICAgcmV0dXJuIHRoaXM7XG4gIH1cblxuICBzZXRIb3Vycyh2YWx1ZTogbnVtYmVyLCBkaXNhYmxlZDogYm9vbGVhbik6IHRoaXMge1xuICAgIGlmIChkaXNhYmxlZCkge1xuICAgICAgcmV0dXJuIHRoaXM7XG4gICAgfVxuICAgIHRoaXMuc2V0RGVmYXVsdFZhbHVlSWZOaWwoKTtcbiAgICB0aGlzLmhvdXJzID0gdmFsdWU7XG4gICAgcmV0dXJuIHRoaXM7XG4gIH1cblxuICBzZXRTZWNvbmRzKHZhbHVlOiBudW1iZXIsIGRpc2FibGVkOiBib29sZWFuKTogdGhpcyB7XG4gICAgaWYgKGRpc2FibGVkKSB7XG4gICAgICByZXR1cm4gdGhpcztcbiAgICB9XG4gICAgdGhpcy5zZXREZWZhdWx0VmFsdWVJZk5pbCgpO1xuICAgIHRoaXMuc2Vjb25kcyA9IHZhbHVlO1xuICAgIHJldHVybiB0aGlzO1xuICB9XG5cbiAgZ2V0IGNoYW5nZXMoKTogT2JzZXJ2YWJsZTxEYXRlPiB7XG4gICAgcmV0dXJuIHRoaXMuX2NoYW5nZXMuYXNPYnNlcnZhYmxlKCk7XG4gIH1cblxuICBnZXQgdmFsdWUoKTogRGF0ZSB8IHVuZGVmaW5lZCB7XG4gICAgcmV0dXJuIHRoaXMuX3ZhbHVlO1xuICB9XG5cbiAgc2V0IHZhbHVlKHZhbHVlOiBEYXRlIHwgdW5kZWZpbmVkKSB7XG4gICAgaWYgKHZhbHVlICE9PSB0aGlzLl92YWx1ZSkge1xuICAgICAgdGhpcy5fdmFsdWUgPSB2YWx1ZTtcbiAgICAgIGlmIChpc05vdE5pbCh0aGlzLl92YWx1ZSkpIHtcbiAgICAgICAgdGhpcy5faG91cnMgPSB0aGlzLl92YWx1ZSEuZ2V0SG91cnMoKTtcbiAgICAgICAgdGhpcy5fbWludXRlcyA9IHRoaXMuX3ZhbHVlIS5nZXRNaW51dGVzKCk7XG4gICAgICAgIHRoaXMuX3NlY29uZHMgPSB0aGlzLl92YWx1ZSEuZ2V0U2Vjb25kcygpO1xuICAgICAgfSBlbHNlIHtcbiAgICAgICAgdGhpcy5fY2xlYXIoKTtcbiAgICAgIH1cbiAgICB9XG4gIH1cblxuICBzZXRWYWx1ZSh2YWx1ZTogRGF0ZSB8IHVuZGVmaW5lZCk6IHRoaXMge1xuICAgIHRoaXMudmFsdWUgPSB2YWx1ZTtcbiAgICByZXR1cm4gdGhpcztcbiAgfVxuXG4gIGNsZWFyKCk6IHZvaWQge1xuICAgIHRoaXMuX2NsZWFyKCk7XG4gICAgdGhpcy51cGRhdGUoKTtcbiAgfVxuXG4gIGdldCBpc0VtcHR5KCk6IGJvb2xlYW4ge1xuICAgIHJldHVybiAhKGlzTm90TmlsKHRoaXMuX2hvdXJzKSB8fCBpc05vdE5pbCh0aGlzLl9taW51dGVzKSB8fCBpc05vdE5pbCh0aGlzLl9zZWNvbmRzKSk7XG4gIH1cblxuICBwcml2YXRlIF9jbGVhcigpOiB2b2lkIHtcbiAgICB0aGlzLl9ob3VycyA9IHVuZGVmaW5lZDtcbiAgICB0aGlzLl9taW51dGVzID0gdW5kZWZpbmVkO1xuICAgIHRoaXMuX3NlY29uZHMgPSB1bmRlZmluZWQ7XG4gIH1cblxuICBwcml2YXRlIHVwZGF0ZSgpOiB2b2lkIHtcbiAgICBpZiAodGhpcy5pc0VtcHR5KSB7XG4gICAgICB0aGlzLl92YWx1ZSA9IHVuZGVmaW5lZDtcbiAgICB9IGVsc2Uge1xuICAgICAgaWYgKCFpc05vdE5pbCh0aGlzLl9ob3VycykpIHtcbiAgICAgICAgdGhpcy5faG91cnMgPSB0aGlzLmRlZmF1bHRIb3VycztcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuX3ZhbHVlIS5zZXRIb3Vycyh0aGlzLmhvdXJzISk7XG4gICAgICB9XG5cbiAgICAgIGlmICghaXNOb3ROaWwodGhpcy5fbWludXRlcykpIHtcbiAgICAgICAgdGhpcy5fbWludXRlcyA9IHRoaXMuZGVmYXVsdE1pbnV0ZXM7XG4gICAgICB9IGVsc2Uge1xuICAgICAgICB0aGlzLl92YWx1ZSEuc2V0TWludXRlcyh0aGlzLm1pbnV0ZXMhKTtcbiAgICAgIH1cblxuICAgICAgaWYgKCFpc05vdE5pbCh0aGlzLl9zZWNvbmRzKSkge1xuICAgICAgICB0aGlzLl9zZWNvbmRzID0gdGhpcy5kZWZhdWx0U2Vjb25kcztcbiAgICAgIH0gZWxzZSB7XG4gICAgICAgIHRoaXMuX3ZhbHVlIS5zZXRTZWNvbmRzKHRoaXMuc2Vjb25kcyEpO1xuICAgICAgfVxuXG4gICAgICB0aGlzLl92YWx1ZSA9IG5ldyBEYXRlKHRoaXMuX3ZhbHVlISk7XG4gICAgfVxuICAgIHRoaXMuY2hhbmdlZCgpO1xuICB9XG5cbiAgY2hhbmdlZCgpOiB2b2lkIHtcbiAgICB0aGlzLl9jaGFuZ2VzLm5leHQodGhpcy5fdmFsdWUpO1xuICB9XG5cbiAgZ2V0IGhvdXJzKCk6IG51bWJlciB8IHVuZGVmaW5lZCB7XG4gICAgcmV0dXJuIHRoaXMuX2hvdXJzO1xuICB9XG5cbiAgc2V0IGhvdXJzKHZhbHVlOiBudW1iZXIgfCB1bmRlZmluZWQpIHtcbiAgICBpZiAodmFsdWUgIT09IHRoaXMuX2hvdXJzKSB7XG4gICAgICB0aGlzLl9ob3VycyA9IHZhbHVlO1xuICAgICAgdGhpcy51cGRhdGUoKTtcbiAgICB9XG4gIH1cblxuICBnZXQgbWludXRlcygpOiBudW1iZXIgfCB1bmRlZmluZWQge1xuICAgIHJldHVybiB0aGlzLl9taW51dGVzO1xuICB9XG5cbiAgc2V0IG1pbnV0ZXModmFsdWU6IG51bWJlciB8IHVuZGVmaW5lZCkge1xuICAgIGlmICh2YWx1ZSAhPT0gdGhpcy5fbWludXRlcykge1xuICAgICAgdGhpcy5fbWludXRlcyA9IHZhbHVlO1xuICAgICAgdGhpcy51cGRhdGUoKTtcbiAgICB9XG4gIH1cblxuICBnZXQgc2Vjb25kcygpOiBudW1iZXIgfCB1bmRlZmluZWQge1xuICAgIHJldHVybiB0aGlzLl9zZWNvbmRzO1xuICB9XG5cbiAgc2V0IHNlY29uZHModmFsdWU6IG51bWJlciB8IHVuZGVmaW5lZCkge1xuICAgIGlmICh2YWx1ZSAhPT0gdGhpcy5fc2Vjb25kcykge1xuICAgICAgdGhpcy5fc2Vjb25kcyA9IHZhbHVlO1xuICAgICAgdGhpcy51cGRhdGUoKTtcbiAgICB9XG4gIH1cblxuICBnZXQgZGVmYXVsdE9wZW5WYWx1ZSgpOiBEYXRlIHtcbiAgICByZXR1cm4gdGhpcy5fZGVmYXVsdE9wZW5WYWx1ZTtcbiAgfVxuXG4gIHNldCBkZWZhdWx0T3BlblZhbHVlKHZhbHVlOiBEYXRlKSB7XG4gICAgaWYgKHRoaXMuX2RlZmF1bHRPcGVuVmFsdWUgIT09IHZhbHVlKSB7XG4gICAgICB0aGlzLl9kZWZhdWx0T3BlblZhbHVlID0gdmFsdWU7XG4gICAgICB0aGlzLnVwZGF0ZSgpO1xuICAgIH1cbiAgfVxuXG4gIHNldERlZmF1bHRPcGVuVmFsdWUodmFsdWU6IERhdGUpOiB0aGlzIHtcbiAgICB0aGlzLmRlZmF1bHRPcGVuVmFsdWUgPSB2YWx1ZTtcbiAgICByZXR1cm4gdGhpcztcbiAgfVxuXG4gIGdldCBkZWZhdWx0SG91cnMoKTogbnVtYmVyIHtcbiAgICByZXR1cm4gdGhpcy5fZGVmYXVsdE9wZW5WYWx1ZS5nZXRIb3VycygpO1xuICB9XG5cbiAgZ2V0IGRlZmF1bHRNaW51dGVzKCk6IG51bWJlciB7XG4gICAgcmV0dXJuIHRoaXMuX2RlZmF1bHRPcGVuVmFsdWUuZ2V0TWludXRlcygpO1xuICB9XG5cbiAgZ2V0IGRlZmF1bHRTZWNvbmRzKCk6IG51bWJlciB7XG4gICAgcmV0dXJuIHRoaXMuX2RlZmF1bHRPcGVuVmFsdWUuZ2V0U2Vjb25kcygpO1xuICB9XG5cbiAgY29uc3RydWN0b3IoKSB7fVxufVxuIl19